package lock.redis;

/**
 *  redis实现分布式悲观锁：
 *      分布式锁：一般用在全局id生成，秒杀系统，全局变量共享、分布式事务等。一般会有两种实现方案，一种是悲观锁的实现，一种是乐观锁的实现。
 *                悲观锁的并发性能差，但是能保证不会发生脏数据的可能性小一点。
 *      redis分布式锁：SETNX命令和GETSET命令（这是一个原子命令！）
 *                    SETNX当且仅当 key 不存在，将 key 的值设为 value ，并返回1；若给定的 key 已经存在，则 SETNX 不做任何动作，并返回0
 *     ******/

import redis.clients.jedis.Jedis;

import java.util.concurrent.TimeUnit;

/*****
 *     获取锁时最好用lock(long time, TimeUnit unit), 以免网路问题而导致线程一直阻塞
 */
public class RedisBasedDistributedLock extends AbstractLock{

     private Jedis jedis;
     //锁的名字
     protected String lockKey;
     //锁的有效时间，单位毫秒
     protected long lockExpires;

    public RedisBasedDistributedLock(Jedis jedis, String lockKey, long lockExpires) {
        this.jedis = jedis;
        this.lockKey = lockKey;
        this.lockExpires = lockExpires;
    }

    @Override
    protected void unlock0() {
        // 判断锁是否过期
        String value = jedis.get(lockKey);
        if (!isTimeExpire(value)) {
            jedis.del(lockKey);
        }
    }



    //阻塞式获取锁的实现
    @Override
    protected boolean lock(boolean useTimeout, long time, TimeUnit unit, boolean interrupt) throws InterruptedException {
        System.out.println("判断线程是否中断...");
        if (interrupt)
            checkInterruption();

        System.out.println("判断是否需要使用超时...");
        long start = System.currentTimeMillis();
        long timeout = unit.toMillis(time);
        //在规定时间内获取锁
        while(useTimeout?isTimeout(start,timeout):true){
            System.out.println("在规定时间获取锁内判断线程是否中断...");
            if (interrupt)
                checkInterruption();

            long lockExpireTime = System.currentTimeMillis()+lockExpires+1; //锁超时时间
            String stringOfLockExpireTime = String.valueOf(lockExpireTime);

            System.out.println("判断是否是第一次获取锁...");
            if (jedis.setnx(lockKey,stringOfLockExpireTime)==1){ //第一次肯定获取锁
                System.out.println("时第一次获取锁，并且设置标志...");
                locked = true;
                setExclusiveOwnerThread(Thread.currentThread());
                return true;
            }

            //获取锁的有效时间
            System.out.println("不是第一次获取锁，需要判断锁是否过期...");
            String value = jedis.get(lockKey);
            if (value!=null && isTimeExpire(value)){  //检查锁是否过期
                System.out.println("锁没有过期，有多个线程同时进入获取锁...");
                // 假设多个线程(非单jvm)同时走到这里
                String oldValue = jedis.getSet(lockKey, stringOfLockExpireTime); //原子操作
                // 但是走到这里时每个线程拿到的oldValue肯定不可能一样(因为getset是原子性的)
                // 加入拿到的oldValue依然是expired的，那么就说明拿到锁了
                System.out.println("判断锁时候超时，不超时可以拿到锁...");
                if (oldValue != null && isTimeExpire(oldValue)) {
                    System.out.println("非第一次拿到锁，设置标识...");
                    //成功获取到锁, 设置相关标识
                    locked = true;
                    setExclusiveOwnerThread(Thread.currentThread());
                    return true;
                }
            } else {
                // TODO lock is not expired, enter next loop retrying
            }
        }
        System.out.println("没有拿到锁...");
        return false;
    }

    @Override
    public boolean tryLock() {
        long lockExpireTime = System.currentTimeMillis() + lockExpires + 1;// 锁超时时间
        String stringOfLockExpireTime = String.valueOf(lockExpireTime);

        if (jedis.setnx(lockKey, stringOfLockExpireTime) == 1) { // 获取到锁
            // 成功获取到锁, 设置相关标识
            locked = true;
            setExclusiveOwnerThread(Thread.currentThread());
            return true;
        }
        String value = jedis.get(lockKey);
        if (value != null && isTimeExpire(value)) { // lock is expired
            // 假设多个线程(非单jvm)同时走到这里
            String oldValue = jedis.getSet(lockKey, stringOfLockExpireTime); //原子操作
            // 但是走到这里时每个线程拿到的oldValue肯定不可能一样(因为getset是原子性的)
            // 假如拿到的oldValue依然是expired的，那么就说明拿到锁了
            if (oldValue != null && isTimeExpire(oldValue)) {
                //成功获取到锁, 设置相关标识
                locked = true;
                setExclusiveOwnerThread(Thread.currentThread());
                return true;
            }
        } else {
            // TODO lock is not expired, enter next loop retrying
        }
        return false;

    }

    public boolean isLocked(){
        if (locked)
            return true;
        else {
            // TODO 这里其实是有问题的, 想:当get方法返回value后, 假设这个value已经是过期的了,
            // 而就在这瞬间, 另一个节点set了value, 这时锁是被别的线程(节点持有), 而接下来的判断
            // 是检测不出这种情况的.不过这个问题应该不会导致其它的问题出现, 因为这个方法的目的本来就
            // 不是同步控制, 它只是一种锁状态的报告.
            String value = jedis.get(lockKey);
            return !isTimeExpire(value);
        }
    }

    private boolean isTimeExpire(String value) {
        return Long.parseLong(value) < System.currentTimeMillis();
    }

    private boolean isTimeout(long start, long timeout) {
        return start+timeout>System.currentTimeMillis();
    }

    private void checkInterruption() throws InterruptedException {
        if (Thread.currentThread().isInterrupted())
            throw new InterruptedException();
    }
}
