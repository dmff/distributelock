package lock.redis;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

/**
 * 锁的骨架
 */
public abstract class AbstractLock implements Lock {

    /**
     * 这里需不需要保证可见性值得讨论, 因为是分布式的锁,
     * 1.同一个jvm的多个线程使用不同的锁对象其实也是可以的, 这种情况下不需要保证可见性
     * 2.同一个jvm的多个线程使用同一个锁对象, 那可见性就必须要保证了.
     */
    protected volatile boolean locked;

    /**
     * 当前jvm持有该锁的线程
     */
    private Thread exclusiveOwnerThread;

    @Override
    public void lock() {
        try {
            lock(false,0,null,false);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        lock(false,0,null,true);
    }

    /**
     *
     * @return false
     */
    @Override
    public boolean tryLock() {
       return false;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit)  {
        System.out.println("start tryLock have arg");
        try {
            return lock(true,time,unit,false);
        } catch (InterruptedException e) {
            e.printStackTrace();
            System.out.println(""+e);
        }
        return false;
    }

    public boolean tryLockInterruptibly(long time, TimeUnit unit) throws InterruptedException {
        return lock(true, time, unit, true);
    }

    @Override
    public void unlock() {
        if (Thread.currentThread() !=getExclusiveOwnerThread())
            throw new IllegalMonitorStateException("current thread does not hold the lock");
        unlock0();
        setExclusiveOwnerThread(null);
    }

    @Override
    public Condition newCondition() {
        return null;
    }

    protected abstract void unlock0();
    /**
     * 阻塞式获取锁的实现
     *
     * @param useTimeout
     * @param time
     * @param unit
     * @param interrupt :是否响应中断
     * @return
     * @throws InterruptedException
     */
    protected abstract boolean lock(boolean useTimeout, long time, TimeUnit unit, boolean interrupt)
            throws InterruptedException;

    public Thread getExclusiveOwnerThread() {
        return exclusiveOwnerThread;
    }

    public void setExclusiveOwnerThread(Thread exclusiveOwnerThread) {
        this.exclusiveOwnerThread = exclusiveOwnerThread;
    }
}
