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
package org.apache.curator.retry;

import org.apache.curator.RetrySleeper;

/**
 * 在给定的时间内允许重试 - 且每次睡眠固定时间。
 *
 * A retry policy that retries until a given amount of time elapses
 */
public class RetryUntilElapsed extends SleepingRetry
{
    /**
     * 操作最长耗时 - 一旦过去指定时间还未成功，则不允许重试。
     */
    private final int maxElapsedTimeMs;
    /**
     * 每次重试期间的睡眠时间 - 固定值
     */
    private final int sleepMsBetweenRetries;

    public RetryUntilElapsed(int maxElapsedTimeMs, int sleepMsBetweenRetries)
    {
        super(Integer.MAX_VALUE);
        this.maxElapsedTimeMs = maxElapsedTimeMs;
        this.sleepMsBetweenRetries = sleepMsBetweenRetries;
    }

    @Override
    public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper)
    {
        // 当过去的总时间 小于 限定总时间时允许重试
        return super.allowRetry(retryCount, elapsedTimeMs, sleeper) && (elapsedTimeMs < maxElapsedTimeMs);
    }

    @Override
    protected long getSleepTimeMs(int retryCount, long elapsedTimeMs)
    {
        // 每次重试 - 固定等待时间
        return sleepMsBetweenRetries;
    }
}

