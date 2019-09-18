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

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 一个<b>不允许重入</b>的跨JVM的互斥信号锁。使用ZooKeeper来保持锁定。所有jvm中使用相同锁路径的所有进程都将实现进程间互斥区。
 * <p>
 *     拓展：其实常见的互斥锁可以看做信号量的一种特殊情况，它的最大信号量为1。也就是说 {@link Semaphore}其实是可以代替{@link ReentrantLock}的，
 *     不过{@link ReentrantLock}具有更清晰的语义。
 *    （共享锁资源数为1的时候，就变成了互斥锁）
 * </p>
 *
 * A NON re-entrant mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes in all JVMs that
 * use the same lock path will achieve an inter-process critical section.
 */
public class InterProcessSemaphoreMutex implements InterProcessLock
{
    private final InterProcessSemaphoreV2 semaphore;
    /**
     * 这里的volatile虽然能保证可见性，但是{@link #release()}的实现并不太安全。
     * 其实应该和{@link InterProcessMutex#threadData}一样，进行数据隔离。
     * 或者流程都是加锁的。
     */
    private volatile Lease lease;

    /**
     * @param client the client
     * @param path path for the lock
     */
    public InterProcessSemaphoreMutex(CuratorFramework client, String path)
    {
        // 资源为1的信号量 - 达成互斥
        this.semaphore = new InterProcessSemaphoreV2(client, path, 1);
    }

    /**
     * {@inheritDoc}
     * 该方法将阻塞到完成
     */
    @Override
    public void acquire() throws Exception
    {
        // 看起来是安全的，因为上一个线程不释放，当前线程就无法获得资源，也就无法赋值
        // 但实际上和release存在竞态条件
        lease = semaphore.acquire();
    }

    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        Lease acquiredLease = semaphore.acquire(time, unit);
        if ( acquiredLease == null )
        {
            // important - don't overwrite lease field if couldn't be acquired
            // 极其重要：在申请失败的情况下不要覆盖当前值
            return false;
        }
        // 这里能保证只有一个线程走到这里 - 如果上一个线程没有释放资源，那么就无法走到这里。
        // 这里同样和release存在竞态条件
        lease = acquiredLease;
        return true;
    }

    @Override
    public void release() throws Exception
    {
        Lease lease = this.lease;
        // 这个检测并不能保证安全性，如果错误的线程调用了release，后续的代码将可能造成错误
        Preconditions.checkState(lease != null, "Not acquired");
        this.lease = null;
        // 释放资源 - 理论上讲，该行代码是由潜在风险的！并不能保证 close() 在 this.lease = null 之后执行！
        // volatile只有写前、读后保证。 - 写volatile之前的代码不会重排序到写volatile之后，读volatile之后的代码不会重排序到读volatile之前。
        // 由于封装的太厉害，无法清晰的分析，执行顺序可能是
        // 1. close()
        // 2. lease = acquire()
        // 3. this.lease = null
        // 如果实际没有出现过，可能是因为网络上的消耗时间较长
        lease.close();
    }

    @Override
    public boolean isAcquiredInThisProcess()
    {
        // 判断是否获得了该锁
        return (lease != null);
    }
}
