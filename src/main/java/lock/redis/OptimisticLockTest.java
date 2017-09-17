package lock.redis;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * redis乐观锁的实例
 *    乐观锁：大多数是基于在数据库增加一个version，update时版本号+1，更新时比较版本号是否一致，不一致为过期数据，需要重新获取
 *    redis乐观锁：redis中可以使用watch命令监视key的变化，在exec时候如果监视的key发生过变化，则整个事务会失败，也可以调用watch多次监视多个key，
 *                这样就可以对指定的key加乐观锁。注意watch的key对整个连接是有效的，事务也一样，如果连接断开，监视和事务会自动清除，
 *                这样exec、discard、unwatch命令都会清除连接中的所有监视
 *
 *    redis事务：redis事务需要用到multi和exec两个命令
 *              1.multi，开启Redis的事务，置客户端为事务态。
 *              2.exec，提交事务，执行从multi到此命令前的命令队列，置客户端为非事务态。
 *              3.discard，取消事务，置客户端为非事务态。
 *              4.watch,监视键值对，作用时如果事务提交exec时发现监视的监视对发生变化，事务将被取消。
 */
public class OptimisticLockTest {

    public static void main(String[] args){
        long starTime=System.currentTimeMillis();

        initProduct();
        initClient();
        printResult();

        long endTime=System.currentTimeMillis();
        long Time=endTime-starTime;
        System.out.println("程序运行时间： "+Time+"ms");
    }

    /**
     * 打印结果
     */
    public static void printResult(){
        Jedis jedis = RedisUtil.getInstance().getJedis();
        Set<String> set = jedis.smembers("clientList");
        int i=1;
        for (String value:set) {
            System.out.println("第" + i++ + "个抢到商品，"+value + " ");
        }
        RedisUtil.returnResource(jedis);
    }

    /**
     * 初始化顾客
     */
    public static void initClient(){
        ExecutorService cacheThreadPool = Executors.newCachedThreadPool();
        for(int i=0;i<1000;i++){
            cacheThreadPool.execute(new ClientThread(i));
        }
        cacheThreadPool.shutdown();
        while(true){
            if (cacheThreadPool.isTerminated()){
                System.out.println("所有的线程都结束了！");
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 初始化商品个数
     */
    public static void initProduct(){
        int proNum = 100;//商品个数
        String key = "proNum";
        String clientList = "clientList"; //抢到商品数量的列表
        Jedis jedis = RedisUtil.getInstance().getJedis();
        if (jedis.exists(key)){
            jedis.del(key);
        }

        if (jedis.exists(clientList)){
            jedis.del(clientList);
        }
        //初始化
        jedis.set(key,String.valueOf(proNum));
        RedisUtil.returnResource(jedis);
    }

    /**
     * 顾客线程
     */
    static class ClientThread implements Runnable{
       Jedis jedis = null;
        String key = "proNum";
        String clientList = "clientList"; //抢到商品数量的列表
        String clientName;

        public ClientThread(int num) {
            this.clientName = "编号="+num;
        }

        @Override
        public void run() {
            try{
                Thread.sleep((long) (Math.random()*5000));
            }catch (InterruptedException e){
                e.printStackTrace();
            }

            while(true){
                System.out.println("顾客："+clientName+" 开始抢商品了");
                jedis = RedisUtil.getInstance().getJedis();
                try{
                    jedis.watch(key);
                    int proNum =Integer.parseInt(jedis.get(key));
                    if (proNum>0){
                        Transaction transaction = jedis.multi();
                        transaction.set(key,String.valueOf(proNum-1));
                        List<Object> result = transaction.exec();
                        if (result==null || result.isEmpty()){
                            System.out.println("悲剧了，顾客:" + clientName + "没有抢到商品");// 可能是watch-key被外部修改，或者是数据操作被驳回
                        }else {
                            jedis.sadd(clientList,clientName); //记录抢到的商品
                            System.out.println("好高兴，顾客:" + clientName + "抢到商品");
                            break;
                        }
                    }else {
                        System.out.println("悲剧了，库存为0，顾客:" + clientName + "没有抢到商品");
                        break;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }finally {
                    jedis.unwatch();
                    RedisUtil.returnResource(jedis);
                }
            }
        }
    }
}
