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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ThreadUtils;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.reverse;

/**
 * 为多个锁提供单个视图。当调用{@link #acquire()}时，它持有的所有锁都会调用{@link #acquire()}。
 * 如果某一个锁申请失败，那么所有申请成功的锁都会调用一次release。
 *
 * 它的好处是可以避免锁顺序死锁（以不同的顺序申请多个锁时，可能导致锁顺序死锁）。
 * 注意：使用多个锁时，顺序申请，逆序释放。
 *
 * A container that manages multiple locks as a single entity. When {@link #acquire()} is called,
 * all the locks are acquired. If that fails, any paths that were acquired are released. Similarly, when
 * {@link #release()} is called, all locks are released (failures are ignored).
 */
public class InterProcessMultiLock implements InterProcessLock
{
    /** 内部持有的锁，是不能修改的 */
    private final List<InterProcessLock> locks;

    /**
     * Creates a multi lock of {@link InterProcessMutex}s
     *
     * @param client the client
     * @param paths list of paths to manage in the order that they are to be locked
     *              需要管理的锁路径，请按照加锁顺序传入。
     */
    public InterProcessMultiLock(CuratorFramework client, List<String> paths)
    {
        // paths get checked in each individual InterProcessMutex, so trust them here
        this(makeLocks(client, paths));
    }

    /**
     * Creates a multi lock of any type of inter process lock
     *
     * @param locks the locks
     */
    public InterProcessMultiLock(List<InterProcessLock> locks)
    {
        this.locks = ImmutableList.copyOf(locks);
    }

    private static List<InterProcessLock> makeLocks(CuratorFramework client, List<String> paths)
    {
        ImmutableList.Builder<InterProcessLock> builder = ImmutableList.builder();
        for ( String path : paths )
        {
            InterProcessLock        lock = new InterProcessMutex(client, path);
            builder.add(lock);
        }
        return builder.build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void acquire() throws Exception
    {
        acquire(-1, null);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean acquire(long time, TimeUnit unit) throws Exception
    {
        // 申请锁时捕获的异常
        Exception                   exception = null;
        // 已申请的锁
        List<InterProcessLock>      acquired = Lists.newArrayList();
        // 是否申请成功，一旦遇见一个失败则失败。
        boolean                     success = true;
        for ( InterProcessLock lock : locks )
        {
            try
            {
                // 无限阻塞，直到锁获取成功
                if ( unit == null )
                {
                    lock.acquire();
                    // 申请成功，加入已申请列表
                    acquired.add(lock);
                }
                else
                {
                    if ( lock.acquire(time, unit) )
                    {
                        // 申请成功，加入已申请列表
                        acquired.add(lock);
                    }
                    else
                    {
                        // 限定时间内没有获得锁，跳出循环，释放申请的锁
                        success = false;
                        break;
                    }
                }
            }
            catch ( Exception e )
            {
                // 出现异常，申请失败(一般而言连接异常)。
                // 这里没有break？？？？ 这里讲道理应该跳出循环，释放申请的锁啊。出现异常不应该继续申请其它锁。
                // 好几次想给curator反馈bug，但是从没找到过反馈bug的地方。
                ThreadUtils.checkInterrupted(e);
                success = false;
                exception = e;
            }
        }
        // 如果没有成功 --- 释放已申请的锁
        if ( !success )
        {
            // 逆序释放，其实记录下标就行。
            for ( InterProcessLock lock : reverse(acquired) )
            {
                try
                {
                    lock.release();
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    // ignore
                }
            }
        }

        if ( exception != null )
        {
            throw exception;
        }
        
        return success;
    }

    /**
     * {@inheritDoc}
     * 注意：释放锁需要逆序释放！
     *
     * 这里的 synchronized 有啥意义呢？？？
     *
     * <p>NOTE: locks are released in the reverse order that they were acquired.</p>
     */
    @Override
    public synchronized void release() throws Exception
    {
        // 释放操作遇见的异常
        Exception       baseException = null;
        // 注意：逆序释放-reverse(locks)
        for ( InterProcessLock lock : reverse(locks) )
        {
            try
            {
                lock.release();
            }
            catch ( Exception e )
            {
                // 可能是没有获得锁，也可能是其它异常(连接异常) --  但是出现异常也必须继续释放其它锁。
                ThreadUtils.checkInterrupted(e);
                if ( baseException == null )
                {
                    baseException = e;
                }
                else
                {
                    baseException = new Exception(baseException);
                }
            }
        }
        // 释放锁的过程中出现了异常
        if ( baseException != null )
        {
            throw baseException;
        }
    }

    /**
     * {@inheritDoc}
     * 这里的 synchronized 有啥意义？？？
     */
    @Override
    public synchronized boolean isAcquiredInThisProcess()
    {
        // it's subjective what the correct meaning is here - I choose to return true
        // only if all of the locks are acquired
        // 什么情况应该返回true是主观的-我选择当且仅当获取了所有锁的时候返回true。

        for ( InterProcessLock lock : locks )
        {
            if ( !lock.isAcquiredInThisProcess() )
            {
                return false;
            }
        }
        return true;
    }
}
