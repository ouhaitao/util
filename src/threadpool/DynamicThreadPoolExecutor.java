package threadpool;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

/**
 * 动态线程池
 * 根据负载情况自动调整核心线程数与最大线程数
 * 扩容是将线程池的核心线程、最大线程数翻倍，分别最大扩容到maxCorePoolSize、maxMaximumPoolSize
 * 缩容是将线程池的核心线程、最大线程数缩小一倍，分别最小缩容到initCorePoolSize、initMaximumPoolSize
 * 扩容条件:
 * 当线程数到达线程池最大线程数,且等待队列的长度大于resizeThreshold就会进行扩容
 * 缩容条件:
 * 当线程池的线程数减少到当前核心线程数,且等待队列为空,
 * 且距离上一次扩容/缩容超过resizeIntervalTime时就会进行缩容
 */
public class DynamicThreadPoolExecutor extends ThreadPoolExecutor {
    
    /**
     * 线程池的初始核心线程数
     */
    private final int initCorePoolSize;
    /**
     * 线程池的初始最大线程数
     */
    private final int initMaximumPoolSize;
    /**
     * 扩容阈值
     */
    private final int resizeThreshold;
    /**
     * 能够扩容到的最大核心线程数
     */
    private volatile int maxCorePoolSize;
    /**
     * 能够扩容的最大线程数
     */
    private volatile int maxMaximumPoolSize;
    /**
     * 上一次扩容/缩容的时间
     */
    private volatile long resizeTime;
    /**
     * 缩容间隔
     */
    private volatile long resizeIntervalTime;
    
    private final AtomicBoolean resizeLock;
    
    private final Thread resizeThread;
    
    public DynamicThreadPoolExecutor(int corePoolSize) {
        this(corePoolSize,
            corePoolSize * 2,
            1, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(corePoolSize));
    }
    
    public DynamicThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                     long keepAliveTime, TimeUnit unit,
                                     BlockingQueue<Runnable> workQueue) {
        this(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, Executors.defaultThreadFactory());
    }
    
    public DynamicThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                     long keepAliveTime, TimeUnit unit,
                                     BlockingQueue<Runnable> workQueue,
                                     ThreadFactory threadFactory) {
        this(corePoolSize, maximumPoolSize,
            keepAliveTime, unit,
            workQueue,
            threadFactory,
            new AbortPolicy());
    }
    
    public DynamicThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                     long keepAliveTime, TimeUnit unit,
                                     BlockingQueue<Runnable> workQueue,
                                     ThreadFactory threadFactory,
                                     RejectedExecutionHandler handler) {
        this(corePoolSize, maximumPoolSize,
            keepAliveTime, unit,
            workQueue,
            threadFactory,
            handler,
            workQueue.remainingCapacity(),
            corePoolSize << 2, maximumPoolSize << 2);
    }
    
    /**
     * @param corePoolSize       线程池参数
     * @param maximumPoolSize    线程池参数
     * @param keepAliveTime      线程池参数
     * @param unit               线程池参数
     * @param workQueue          线程池参数
     * @param threadFactory      线程池参数
     * @param resizeThreshold    扩容阈值
     * @param maxCorePoolSize    最大扩容到的核心线程数
     * @param maxMaximumPoolSize 最大扩容到的最大线程数
     */
    public DynamicThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
                                     long keepAliveTime, TimeUnit unit,
                                     BlockingQueue<Runnable> workQueue,
                                     ThreadFactory threadFactory,
                                     RejectedExecutionHandler handler,
                                     int resizeThreshold, int maxCorePoolSize, int maxMaximumPoolSize) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new ResizePolicy(handler));
        if (resizeThreshold <= 0 || resizeThreshold > workQueue.remainingCapacity()) {
            throw new IllegalArgumentException();
        }
        if (maxCorePoolSize < 0 || maxCorePoolSize < corePoolSize) {
            throw new IllegalArgumentException();
        }
        if (maxMaximumPoolSize < 0 || maxMaximumPoolSize < maximumPoolSize) {
            throw new IllegalArgumentException();
        }
        this.initCorePoolSize = corePoolSize;
        this.initMaximumPoolSize = maximumPoolSize;
        this.resizeTime = System.nanoTime();
        this.resizeIntervalTime = unit.toNanos(keepAliveTime);
        this.resizeThreshold = resizeThreshold;
        this.maxCorePoolSize = maxCorePoolSize;
        this.maxMaximumPoolSize = maxMaximumPoolSize;
        this.resizeLock = new AtomicBoolean(false);
        this.resizeThread = new Thread(() -> {
            while (true) {
                LockSupport.park(resizeIntervalTime);
                lock();
                resize();
                unlock();
            }
        });
        resizeThread.setDaemon(true);
        resizeThread.start();
    }
    
    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        int maximumPoolSize = getMaximumPoolSize();
        int activeCount = getActiveCount();
        int corePoolSize = getCorePoolSize();
        int workerQueueSize = getQueue().size();
        
        // 扩容
        if ((corePoolSize < maxCorePoolSize || maximumPoolSize < maxMaximumPoolSize)
            && activeCount >= maximumPoolSize
            && workerQueueSize >= resizeThreshold
            && !resizeLock.get()) {
            LockSupport.unpark(resizeThread);
        }
    
        // 缩容
        if ((corePoolSize > initCorePoolSize || maximumPoolSize > initMaximumPoolSize)
            && activeCount <= corePoolSize
            && workerQueueSize == 0
            && !resizeLock.get()) {
            LockSupport.unpark(resizeThread);
        }
    }
    
    /**
     * 扩容/缩容
     */
    private ResizeResult resize() {
        ResizeResult resizeResult = ResizeResult.FAIL;
        long nanoTime = System.nanoTime();
        // 自旋
        int maximumPoolSize = getMaximumPoolSize();
        int activeCount = getActiveCount();
        int corePoolSize = getCorePoolSize();
        int workerQueueSize = getQueue().size();
        // 扩容
        if ((corePoolSize < maxCorePoolSize || maximumPoolSize < maxMaximumPoolSize)
            && activeCount >= maximumPoolSize
            && workerQueueSize >= resizeThreshold) {
        
            int newCorePoolSize = corePoolSize << 1;
            if (newCorePoolSize < 0 || newCorePoolSize > maxCorePoolSize) {
                newCorePoolSize = maximumPoolSize;
            }
            int newMaximumPoolSize = maximumPoolSize << 1;
            if (newMaximumPoolSize < 0) {
                newMaximumPoolSize = maxMaximumPoolSize;
            }
            setCorePoolSize(newCorePoolSize);
            setMaximumPoolSize(newMaximumPoolSize);
            resizeTime = nanoTime;
            resizeResult = ResizeResult.SUCCESS;
        }
        // 缩容
        if ((corePoolSize > initCorePoolSize || maximumPoolSize > initMaximumPoolSize)
            && activeCount <= corePoolSize
            && workerQueueSize == 0
            && nanoTime - resizeTime >= resizeIntervalTime) {
        
            int newCorePoolSize = Math.max(corePoolSize >> 1, initCorePoolSize);
            int newMaximumPoolSize = Math.max(maximumPoolSize >> 1, initMaximumPoolSize);
            setCorePoolSize(newCorePoolSize);
            setMaximumPoolSize(newMaximumPoolSize);
            resizeTime = nanoTime;
            resizeResult = ResizeResult.SUCCESS;
        }
        if (resizeResult == ResizeResult.SUCCESS) {
            System.out.printf("resize后当前corePoolSize=%s 当前maximumPoolSize=%s 下次扩容阈值=%s 当前存活线程数=%s 等待队列长度=%s %n", getCorePoolSize(), getMaximumPoolSize(), resizeThreshold, getActiveCount(), getQueue().size());
        }
        return resizeResult;
    }
    
    protected boolean lock() {
        return resizeLock.compareAndSet(false, true);
    }
    
    protected boolean isLock() {
        return resizeLock.get();
    }
    
    protected void unlock() {
        resizeLock.set(false);
    }
    
    public void setMaxCorePoolSize(int maxCorePoolSize) {
        if (maxCorePoolSize <= 0 || maxCorePoolSize < initCorePoolSize) {
            throw new IllegalArgumentException();
        }
        this.maxCorePoolSize = maxCorePoolSize;
    }
    
    public void setMaxMaximumPoolSize(int maxMaximumPoolSize) {
        if (maxMaximumPoolSize < 0 || maxMaximumPoolSize < initMaximumPoolSize) {
            throw new IllegalArgumentException();
        }
        this.maxMaximumPoolSize = maxMaximumPoolSize;
    }
    
    public void setResizeIntervalTime(long resizeIntervalTime) {
        this.resizeIntervalTime = resizeIntervalTime;
    }
    
    protected long getResizeTime() {
        return resizeTime;
    }
    
    protected static class ResizePolicy implements RejectedExecutionHandler {
    
        private RejectedExecutionHandler handler;
    
        public ResizePolicy(RejectedExecutionHandler handler) {
            this.handler = handler;
        }
    
        @Override
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            DynamicThreadPoolExecutor executor = (DynamicThreadPoolExecutor) e;
            while (!executor.isLock()) {
                for (int i = 0; i < 100; i++) {
                    if (executor.lock()) {
                        break;
                    }
                }
                Thread.yield();
            }
            try {
                if (executor.resize() == ResizeResult.SUCCESS) {
                    // 扩容成功重新提交
                    e.execute(r);
                } else {
                    // 没有扩容成功执行拒绝策略
                    handler.rejectedExecution(r, e);
                }
            } finally {
                executor.unlock();
            }
        }
    }
    
    /**
     * 扩容结果
     */
    protected enum ResizeResult {
        /**
         * 扩容成功
         */
        SUCCESS,
        /**
         * 扩容失败
         */
        FAIL;
    }
    
    public static void main(String[] args) {
        DynamicThreadPoolExecutor dynamicThreadPoolExecutor =
            new DynamicThreadPoolExecutor(2, 2, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(2));
        for (int i = 1; i < 10; i++) {
            int finalI = i;
            Runnable task = () -> {
                try {
                    System.out.println("第" + finalI + "个线程启动");
                    Thread.sleep(finalI * 1000L);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    System.out.println("第" + finalI + "个线程异常");
                }
                System.out.println("第" + finalI + "个线程退出");
            };
            System.out.println("第" + finalI + "个线程提交");
            dynamicThreadPoolExecutor.execute(task);
            System.out.println(dynamicThreadPoolExecutor.getQueue().size());
//            try {
//                Thread.sleep(1000L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
        }
    }
}
