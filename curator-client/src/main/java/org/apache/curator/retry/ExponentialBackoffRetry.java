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

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Random;

/**
 * 指数级退避重试策略 - 在两次重试期间增加睡眠时间。
 *
 * Retry policy that retries a set number of times with increasing sleep time between retries
 */
public class ExponentialBackoffRetry extends SleepingRetry
{
    private static final Logger     log = LoggerFactory.getLogger(ExponentialBackoffRetry.class);

    /**
     * 最大重试次数
     * curator这里的设计不太科学 - 指数可以限制为29及以内，但是不应该限制最大重试次数 - 否则即使有最大睡眠时间限制，也无法增加重试次数。
     * eg: 我想每次最多睡眠5S秒中，但是尝试100次，不支持。。。 悄悄的给你转为了29次
     */
    private static final int MAX_RETRIES_LIMIT = 29;
    /**
     * 默认最大睡眠时间
     */
    private static final int DEFAULT_MAX_SLEEP_MS = Integer.MAX_VALUE;

    private final Random random = new Random();
    /**
     * 基础睡眠时间
     */
    private final int baseSleepTimeMs;
    /**
     * 每次等待最大睡眠时间
     */
    private final int maxSleepMs;

    /**
     * @param baseSleepTimeMs initial amount of time to wait between retries
     * @param maxRetries max number of times to retry
     */
    public ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries)
    {
        this(baseSleepTimeMs, maxRetries, DEFAULT_MAX_SLEEP_MS);
    }

    /**
     * @param baseSleepTimeMs initial amount of time to wait between retries
     * @param maxRetries max number of times to retry
     * @param maxSleepMs max time in ms to sleep on each retry
     */
    public ExponentialBackoffRetry(int baseSleepTimeMs, int maxRetries, int maxSleepMs)
    {
        // 这里或多或少不科学 - 该参数应该直接传递给父类，在计算sleepTimeMs的时候限制指数
        super(validateMaxRetries(maxRetries));
        this.baseSleepTimeMs = baseSleepTimeMs;
        this.maxSleepMs = maxSleepMs;
    }

    @VisibleForTesting
    public int getBaseSleepTimeMs()
    {
        return baseSleepTimeMs;
    }

    @Override
    protected long getSleepTimeMs(int retryCount, long elapsedTimeMs)
    {
        // copied from Hadoop's RetryPolicies.java
        // 这也有bug，两个int并不能安全的相乘得到long,会溢出。我测试过该代码（只计算，不sleep），确实会越界。
        // 不过，可能等不到溢出，线程已经睡死了(sleep太久）。。
        long sleepMs = baseSleepTimeMs * Math.max(1, random.nextInt(1 << (retryCount + 1)));
        if ( sleepMs > maxSleepMs )
        {
            log.warn(String.format("Sleep extension too large (%d). Pinning to %d", sleepMs, maxSleepMs));
            sleepMs = maxSleepMs;
        }
        return sleepMs;
    }

    /**
     * 该方法其实应该用在{@link #getSleepTimeMs(int, long)}里计算睡眠时间的时候，而不是用在构造方法里把用户的最大重试次数给悄悄替换了。
     * @param maxRetries 用户期望的最大重试次数
     * @return 最大次数的修正值
     */
    private static int validateMaxRetries(int maxRetries)
    {
        if ( maxRetries > MAX_RETRIES_LIMIT )
        {
            log.warn(String.format("maxRetries too large (%d). Pinning to %d", maxRetries, MAX_RETRIES_LIMIT));
            maxRetries = MAX_RETRIES_LIMIT;
        }
        return maxRetries;
    }
}
