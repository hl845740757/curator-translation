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
package org.apache.curator.framework.recipes.atomic;

import org.apache.curator.framework.CuratorFramework;

/**
 * 用于debug追踪操作的状态/信息；
 *
 * Debugging stats about operations
 */
public class AtomicStats
{
    /**
     * 乐观锁的尝试次数；
     * 在zookeeper中，乐观锁即 带版本号的操作，只有版本号一致的时候才可以操作数据；
     * eg:{@link org.apache.curator.framework.api.SetDataBuilder#withVersion(int)}
     */
    private int     optimisticTries = 0;
    /**
     * 互斥锁尝试次数；
     * 在zookeeper中，互斥锁本质为抢占节点控制权，实现互斥知道的有两种
     * 1.创建临时节点成功获得锁
     * 2.成为第一个临时顺序节点时获得锁
     * 只有拥有控制权的客户端可以操作数据；
     * eg:{@link org.apache.curator.framework.recipes.locks.InterProcessMutex}
     */
    private int     promotedLockTries = 0;
    /** 乐观锁成功时消耗的时间(毫秒) */
    private long    optimisticTimeMs = 0;
    /** 互斥锁成功时消耗的时间(毫秒) */
    private long    promotedTimeMs = 0;

    /**
     * 返回乐观锁的尝试次数。
     *
     * Returns the number of optimistic locks used to perform the operation
     *
     * @return qty
     */
    public int getOptimisticTries()
    {
        return optimisticTries;
    }

    /**
     * 返回互斥锁的尝试次数。
     *
     * Returns the number of mutex locks used to perform the operation
     *
     * @return qty
     */
    public int getPromotedLockTries()
    {
        return promotedLockTries;
    }

    /**
     * 返回乐观锁成功时花费的时间(毫秒)。
     *
     * Returns the time spent trying the operation with optimistic locks
     *
     * @return time in ms
     */
    public long getOptimisticTimeMs()
    {
        return optimisticTimeMs;
    }

    /**
     * 返回互斥锁成功时花费的时间(毫秒)。
     * Returns the time spent trying the operation with mutex locks
     *
     * @return time in ms
     */
    public long getPromotedTimeMs()
    {
        return promotedTimeMs;
    }

    void incrementOptimisticTries()
    {
        ++optimisticTries;
    }

    void incrementPromotedTries()
    {
        ++promotedLockTries;
    }

    void setOptimisticTimeMs(long optimisticTimeMs)
    {
        this.optimisticTimeMs = optimisticTimeMs;
    }

    void setPromotedTimeMs(long promotedTimeMs)
    {
        this.promotedTimeMs = promotedTimeMs;
    }
}
