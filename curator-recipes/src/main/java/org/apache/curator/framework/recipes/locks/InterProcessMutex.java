/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.curator.framework.recipes.locks;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.utils.PathUtils;

/**
 * 进程级的互斥锁。
 * 这是一个跨越JVM的可重入锁（可重入分布式锁）。使用zookeeper持有锁。
 * 所有使用相同锁路径的JVM中的所有进程都将实现进程间的“临界区”。
 * 此外，这个互斥是“公平的”—每个用户将按请求的顺序获得互斥（从zk的角度看）
 *
 * A re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section. Further, this mutex is
 * "fair" - each user will get the mutex in the order requested (from ZK's point of view)
 */
public class InterProcessMutex implements InterProcessLock, Revocable<InterProcessMutex>
{
    /** 锁的内部结构 */
    private final LockInternals internals;
    /** 锁的路径 */
    private final String basePath;
    /**
     * 本地线程的锁信息。
     * 但凡使用Thread对象来访问的map，本质上都类似threadLocal。
     * LockData是线程封闭的，内部属性并不会被外部范围，因此其内部属性其实是不需要特殊处理的。
     */
    private final ConcurrentMap<Thread, LockData> threadData = Maps.newConcurrentMap();

    /**
     * 每个线程的锁信息
     */
    private static class LockData
    {
        /** 与该数据绑定的线程 */
        final Thread owningThread;
        /** 当前线程抢占的节点路径 */
        final String lockPath;
        /**
         * 锁的获得次数！实现重入！
         * 这是一个很重要的优化，获得锁以后，锁的重入其实是在本地完成的。
         * 只有在第一次申请锁和最后一次释放锁的时候才会真正与zookeeper通信！
         * 这个属性好像并不会被其它线程访问啊 -- 定义成atomic容易造成误解啊。可能只是想定义成final吧。
         */
        final AtomicInteger lockCount = new AtomicInteger(1);

        private LockData(Thread owningThread, String lockPath)
        {
            this.owningThread = owningThread;
            this.lockPath = lockPath;
        }
    }

    /**
     * 锁的名字（子节点的前缀）
     */
    private static final String LOCK_NAME = "lock-";

    /**
     * @param client client
     * @param path   the path to lock 互斥锁节点路径。
     */
    public InterProcessMutex(CuratorFramework client, String path)
    {
        this(client, path, new StandardLockInternalsDriver());
    }

    /**
     * @param client client
     * @param path   the path to lock 互斥锁节点路径。
     * @param driver lock driver 锁驱动
     */
    public InterProcessMutex(CuratorFramework client, String path, LockInternalsDriver driver)
    {
        // 1 表示最多只能有一个线程能获得该锁，意味着完全互斥
        this(client, path, LOCK_NAME, 1, driver);
    }

    /**
     * 尝试获得互斥锁 - 阻塞直到锁可用。
     * 注意：同一个线程可以调用acquire实现重入，每一次acquire调用，必须通过{@link #release()}调用来进行平衡。
     * @see InterProcessLock#acquire()
     *
     * Acquire the mutex - blocking until it's available. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception ZK errors, connection interruptions
     */
    @Override
    public void acquire() throws Exception
    {
        if ( !internalLock(-1, null) )
        {
            throw new IOException("Lost connection while trying to acquire lock: " + basePath);
        }
    }

    /**
     * 尝试在限定时间内获得锁 - 阻塞直到锁可用或超时。
     * 注意：同一个线程可以调用acquire实现重入，每一次acquire调用返回true时，必须通过{@link #release()}调用来进行平衡。
     *
     * Acquire the mutex - blocks until it's available or the given time expires. Note: the same thread
     * can call acquire re-entrantly. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        return internalLock(time, unit);
    }

    /**
     * 当JVM的某个线程获得了该互斥锁时返回true。
     *
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    @Override
    public boolean isAcquiredInThisProcess()
    {
        return (threadData.size() > 0);
    }

    /**
     * 如果方法的调用者线程是获得锁的线程，则在互斥锁上执行一次释放操作。如果线程多次调用了acquire方法，
     * 那么在该方法返回之后，将仍然获得该互斥锁。
     *
     * Perform one release of the mutex if the calling thread is the same thread that acquired it. If the
     * thread had made multiple calls to acquire, the mutex will still be held when this method returns.
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    @Override
    public void release() throws Exception
    {
        /*
            Note on concurrency: a given lockData instance
            can be only acted on by a single thread so locking isn't necessary
         */
        // 每个线程只会操作自己先的lockData，因此对lockData加锁是不必要的 ---- 想想ThreadLocal

        Thread currentThread = Thread.currentThread();
        LockData lockData = threadData.get(currentThread);
        if ( lockData == null )
        {
            // 1. 当且线程没有获得锁，因此不能执行释放操作。 不论JVM内的锁，还是分布式锁，只有获得锁的人才能释放锁
            // 2. 锁的获取次数与释放次数不平衡(匹配)，释放次数大于真正获取锁的次数，存在代码错误
            throw new IllegalMonitorStateException("You do not own the lock: " + basePath);
        }

        int newLockCount = lockData.lockCount.decrementAndGet();
        if ( newLockCount > 0 )
        {
            // 重入次数减1之后，仍然大于0，表示锁重入对应的释放操作，这时只是本地计数减1，与远程无关。
            return;
        }
        if ( newLockCount < 0 )
        {
            // 重入次数减1之后，小于0，表示锁的获取次数与释放次数不平衡(匹配)，释放次数大于真正获取锁的次数，存在代码错误
            // lockData == null 不应该已经包含该情况了吗？ 感觉这里应是走不到的。
            throw new IllegalMonitorStateException("Lock count has gone negative for lock: " + basePath);
        }
        try
        {
            // 重入次数减1之后为0，表示彻底释放锁，
            internals.releaseLock(lockData.lockPath);
        }
        finally
        {
            // 彻底释放锁之后，删除对应的LockData
            threadData.remove(currentThread);
        }
    }

    /**
     * 获取锁路径下的所有参与锁竞争的节点的有序列表。
     * (锁路径下可以存在普通节点，也可以存在正在竞争锁的节点，需要进行过滤以及排序)
     *
     * Return a sorted list of all current nodes participating in the lock
     *
     * @return list of nodes 参与锁竞争的节点列表。
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<String> getParticipantNodes() throws Exception
    {
        return LockInternals.getParticipantNodes(internals.getClient(), basePath, internals.getLockName(), internals.getDriver());
    }

    /** @see Revocable#makeRevocable(RevocationListener)  */
    @Override
    public void makeRevocable(RevocationListener<InterProcessMutex> listener)
    {
        makeRevocable(listener, MoreExecutors.sameThreadExecutor());
    }

    /** @see Revocable#makeRevocable(RevocationListener, Executor)  */
    @Override
    public void makeRevocable(final RevocationListener<InterProcessMutex> listener, Executor executor)
    {
        internals.makeRevocable(new RevocationSpec(executor, new Runnable()
            {
                @Override
                public void run()
                {
                    listener.revocationRequested(InterProcessMutex.this);
                }
            }));
    }

    /**
     * 为子类和包内其它对象使用的构造方法
     * @param client curator客户端
     * @param path 锁的路径
     * @param lockName 锁的名字 --- 子节点名字的前缀
     * @param maxLeases 最大租约数 -- 最多同时几个线程(可以跨JVM)能获得该锁，1 表示完全互斥，大于1时表示可以多个线程可以同时申请资源
     *                  ---> 可以实现{@link java.util.concurrent.Semaphore} {@link java.util.concurrent.locks.ReadWriteLock}
     *                  --- 其实Lock是Semaphore的一种特殊情况。
     *                  --- lease这个名字不知道大家觉得是否形象？
     * @param driver 锁驱动
     */
    InterProcessMutex(CuratorFramework client, String path, String lockName, int maxLeases, LockInternalsDriver driver)
    {
        basePath = PathUtils.validatePath(path);
        internals = new LockInternals(client, driver, path, lockName, maxLeases);
    }

    /**
     * 查询是否当前线程获得了该锁
     * @return 如果当前线程获得了锁，则返回true，否则返回false
     */
    boolean isOwnedByCurrentThread()
    {
        // 存为临时变量的意义：保证前后两次判断时数据的一致性 -- 在多线程代码中很常见。 (存为临时变量，或成为另一个方法的参数)
        LockData lockData = threadData.get(Thread.currentThread());
        return (lockData != null) && (lockData.lockCount.get() > 0);
    }

    protected byte[] getLockNodeBytes()
    {
        return null;
    }

    /**
     * 获取当前线程对应的锁节点路径
     * @return 如果当前线程还没有开始申请锁，则返回null，否则返回当前线程对应的节点路径
     */
    protected String getLockPath()
    {
        LockData lockData = threadData.get(Thread.currentThread());
        return lockData != null ? lockData.lockPath : null;
    }

    private boolean internalLock(long time, TimeUnit unit) throws Exception
    {
        /*
           Note on concurrency: a given lockData instance
           can be only acted on by a single thread so locking isn't necessary
        */
        Thread currentThread = Thread.currentThread();

        LockData lockData = threadData.get(currentThread);
        if ( lockData != null )
        {
            // re-entering 已获得锁，锁重入，在本地完成！这是个重要的优化！
            // 不论采用redis，数据库实现分布式锁，重入都可以这样
            lockData.lockCount.incrementAndGet();
            return true;
        }
        String lockPath = internals.attemptLock(time, unit, getLockNodeBytes());
        // 获得锁成功
        if ( lockPath != null )
        {
            // 标记为当前线程获得了该分布式锁
            LockData newLockData = new LockData(currentThread, lockPath);
            threadData.put(currentThread, newLockData);
            return true;
        }

        return false;
    }
}
