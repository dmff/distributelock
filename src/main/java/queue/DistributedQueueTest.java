package queue;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;

import java.io.Serializable;

/**
 * zk实现两种类型的队列：
 *      1.同步队列：当一个队列的成员都聚齐时，这个队列才可用，否则一直等待所有成员到达。
 *                 例如一个班去旅游，看是否所有人都到齐了，到齐了就发车。例如有个大任务分解为多个子任务，要所有子任务都完成了才能进入到下一流程
 *      2.先进先出队列：
 *              按照FIFO方式进行入队和出队
 *              例如实现生产者和消费者模型
 */
public class DistributedQueueTest {
    
    public static void main(String[] args){
        ZkClient zkClient = new ZkClient("192.168.56.110:2181", 5000, 5000, new SerializableSerializer());
        DistributedSimpleQueue<SendObject> queue = new DistributedSimpleQueue<SendObject>(zkClient, "/Queue");
        new Thread(new ConsumerThread(queue)).start();
        new Thread(new ProducerThread(queue)).start();
    }

    static class ConsumerThread implements Runnable {
        private DistributedSimpleQueue<SendObject> queue;

        public ConsumerThread(DistributedSimpleQueue<SendObject> queue) {
            this.queue = queue;
        }

        public void run() {
            for (int i = 0; i < 10; i++) {
                try {
                    Thread.sleep((int) (Math.random() * 5000));// 随机睡眠一下
                    SendObject sendObject = (SendObject) queue.poll();
                    System.out.println("消费一条消息成功：" + sendObject);
                } catch (Exception e) {
                }
            }
        }
    }

    static class ProducerThread implements Runnable {

        private DistributedSimpleQueue<SendObject> queue;

        public ProducerThread(DistributedSimpleQueue<SendObject> queue) {
            this.queue = queue;
        }

        public void run() {
            for (int i = 0; i < 10; i++) {
                try {
                    Thread.sleep((int) (Math.random() * 5000));// 随机睡眠一下
                    SendObject sendObject = new SendObject(String.valueOf(i), "content" + i);
                    queue.offer(sendObject);
                    System.out.println("发送一条消息成功：" + sendObject);
                } catch (Exception e) {
                }
            }
        }

    }

    static class SendObject implements Serializable {

        private static final long serialVersionUID = 1L;

        public SendObject(String id, String content) {
            this.id = id;
            this.content = content;
        }

        private String id;

        private String content;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getContent() {
            return content;
        }

        public void setContent(String content) {
            this.content = content;
        }

        @Override
        public String toString() {
            return "SendObject [id=" + id + ", content=" + content + "]";
        }
    }
}
