package org.redisson.yunai;

import org.redisson.Redisson;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LockTest {

    private static final String LOCK_KEY = "anylock";

//    @Autowired
//    private RedissonClient redissonClient;
//
//    @Test
//    public void test() throws InterruptedException {
//        // 启动一个线程 A ，去占有锁
//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                // 加锁以后 10 秒钟自动解锁
//                // 无需调用 unlock 方法手动解锁
//                final RLock lock = redissonClient.getLock(LOCK_KEY);
//                lock.lock(10, TimeUnit.SECONDS);
//            }
//        }).start();
//        // 简单 sleep 1 秒，保证线程 A 成功持有锁
//        Thread.sleep(1000L);
//
//        // 尝试加锁，最多等待 100 秒，上锁以后 10 秒自动解锁
//        System.out.println(String.format("准备开始获得锁时间：%s", new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").format(new Date())));
//        final RLock lock = redissonClient.getLock(LOCK_KEY);
//        boolean res = lock.tryLock(100, 10, TimeUnit.SECONDS);
//        if (res) {
//            System.out.println(String.format("实际获得锁时间：%s", new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").format(new Date())));
//        } else {
//            System.out.println("加锁失败");
//        }
//    }

    public static void main(String[] args) throws InterruptedException {
        // 创建 RedissonClient 对象
        RedissonClient redissonClient = Redisson.create();
        System.out.println(String.format("准备开始获得锁时间：%s", new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").format(new Date())));
        final RLock lock = redissonClient.getLock(LOCK_KEY);
//        boolean res = lock.tryLock(200, 100, TimeUnit.SECONDS);
//        if (res) {
//            System.out.println(String.format("实际获得锁时间：%s", new SimpleDateFormat("yyyy-MM-DD HH:mm:ss").format(new Date())));
//        } else {
//            System.out.println("加锁失败");
//        }
        lock.tryLock(100, 30, TimeUnit.SECONDS);

        if (true) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    final RLock lock = redissonClient.getLock(LOCK_KEY);
                    try {
//                        lock.tryLockAsync(100, 100, TimeUnit.SECONDS).get();
                        lock.tryLockAsync().get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }

        Thread.sleep(10 * 1000L);

        lock.unlock();

        Thread.sleep(1000000L);
    }

}
