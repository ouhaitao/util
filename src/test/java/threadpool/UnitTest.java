package threadpool;

import org.junit.jupiter.api.Test;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class UnitTest {
    
    /**
     * 测试高并发
     */
    @Test
    public void testHighConcurrency() throws InterruptedException {
        int taskCount = 100;
        int corePoolSize = 4, maximumPoolSize = 8;
        int resizeIntervalTime = 1;
        TimeUnit resizeIntervalTimeUtil = TimeUnit.SECONDS;
        DynamicThreadPoolExecutor executor = new DynamicThreadPoolExecutor(
            corePoolSize, maximumPoolSize, 1, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(taskCount - (maximumPoolSize << 2)),
            resizeIntervalTime, resizeIntervalTimeUtil);
    
        CountDownLatch latch = new CountDownLatch(taskCount);
        Random random = new Random();
        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                try {
                    Thread.sleep(random.nextInt(500));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        executor.shutdown();
        boolean terminated = executor.awaitTermination(1, TimeUnit.MINUTES);
        assertTrue(terminated, "未正确终止线程池");
    }
    
    /**
     * 测试自动扩容与缩容
     */
    @Test
    public void testResizeBehavior() throws InterruptedException {
        int taskCount = 100;
        int corePoolSize = 4, maximumPoolSize = 8;
        int resizeIntervalTime = 1;
        TimeUnit resizeIntervalTimeUtil = TimeUnit.SECONDS;
        DynamicThreadPoolExecutor executor = new DynamicThreadPoolExecutor(
            corePoolSize, maximumPoolSize, 1, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(taskCount - (maximumPoolSize << 2)),
            resizeIntervalTime, resizeIntervalTimeUtil);
    
        CountDownLatch latch = new CountDownLatch(taskCount);
        Random random = new Random();
        for (int i = 0; i < taskCount; i++) {
            executor.submit(() -> {
                try {
                    Thread.sleep(random.nextInt(500));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    latch.countDown();
                }
            });
        }
        latch.await();
        System.out.printf("任务执行完成后corePoolSize=%s 当前maximumPoolSize=%s 当前存活线程数=%s 等待队列长度=%s %n",
            executor.getCorePoolSize(), executor.getMaximumPoolSize(), executor.getActiveCount(), executor.getQueue().size());
        Thread.sleep(resizeIntervalTimeUtil.toMillis(resizeIntervalTime) * 3);
        System.out.printf("等待自动缩容后当前corePoolSize=%s 当前maximumPoolSize=%s 当前存活线程数=%s 等待队列长度=%s %n",
            executor.getCorePoolSize(), executor.getMaximumPoolSize(), executor.getActiveCount(), executor.getQueue().size());
    
        assertEquals(corePoolSize, executor.getCorePoolSize());
        assertEquals(maximumPoolSize, executor.getMaximumPoolSize());
        executor.shutdown();
        boolean terminated = executor.awaitTermination(1, TimeUnit.MINUTES);
        assertTrue(terminated, "未正确终止线程池");
    }
    
    /**
     * 测试溢出
     */
    @Test
    public void testQueueOverflow() {
        DynamicThreadPoolExecutor executor = new DynamicThreadPoolExecutor(
            1, 1, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1));
        executor.setMaxCorePoolSize(1);
        executor.setMaxMaximumPoolSize(1);
        
        int rejectedTaskCount = 0;
        for (int i = 0; i < 10; i++) {
            try {
                executor.submit(() -> {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            } catch (RejectedExecutionException e) {
                rejectedTaskCount++;
            }
        }
        
        assertEquals(8, rejectedTaskCount);
        executor.shutdown();
    }
}
