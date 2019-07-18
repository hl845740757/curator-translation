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

import java.util.concurrent.TimeUnit;

/**
 * 进程级别的锁，API很像 {@link java.util.concurrent.locks.Lock}
 */
public interface InterProcessLock
{
    /**
     * 尝试获得互斥锁 - 阻塞直到锁可用。
     * 注意：每一次acquire调用，必须通过{@link #release()}调用来进行平衡。
     * (说人话：必须有相同次数的{@link #release()} 操作)
     * (这里的文档暗示了，会出现可重入锁)
     *
     * 互斥锁的代码模板(写多线程代码一定要注意)：
     * <pre>
     * <code>
     *
     *     lock.acquire();
     *     // 到这里表示锁获取成功
     *     try {
     *
     *     } finally {
     *        // 获取成功必须对应一次release，需要放在finally块中执行。
     *        lock.release();
     *     }
     *
     * </code>
     * </pre>
     *
     *
     * Acquire the mutex - blocking until it's available. Each call to acquire must be balanced by a call
     * to {@link #release()}
     *
     * @throws Exception ZK errors, connection interruptions
     */
    public void acquire() throws Exception;

    /**
     * 尝试在限定时间内获得锁 - 阻塞直到锁可用或超时。
     *  注意：每一次acquire调用返回true时，必须通过{@link #release()}调用来进行平衡。
     *
     * Acquire the mutex - blocks until it's available or the given time expires. Each call to acquire that returns true must be balanced by a call
     * to {@link #release()}
     *
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    public boolean acquire(long time, TimeUnit unit) throws Exception;

    /**
     * 在互斥锁上执行一次release操作。
     * (如果release次数与acquire相同，则真正释放锁)
     *
     * Perform one release of the mutex.
     *
     * @throws Exception ZK errors, interruptions, current thread does not own the lock
     */
    public void release() throws Exception;

    /**
     * 查询该进程(JVM的某一个线程)是否获得了该互斥锁。
     * (暗示了当该进程获得了锁以后，后续的锁操作可能会做优化)
     *
     * Returns true if the mutex is acquired by a thread in this JVM
     *
     * @return true/false
     */
    boolean isAcquiredInThisProcess();
}
