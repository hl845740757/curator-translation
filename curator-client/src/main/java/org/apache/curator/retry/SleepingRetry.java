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

import org.apache.curator.RetryPolicy;
import org.apache.curator.RetrySleeper;

import java.util.concurrent.TimeUnit;

/**
 * 睡眠一段时间再进行重试的重试策略。
 * 它是一个抽象类 - 制定了一个模板。
 */
abstract class SleepingRetry implements RetryPolicy {
    /**
     * 最大重试次数
     */
    private final int n;

    protected SleepingRetry(int n) {
        this.n = n;
    }

    // made public for testing
    public int getN() {
        return n;
    }

    public boolean allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper) {
        if (retryCount < n) {
            // 还可以继续重试
            try {
                sleeper.sleepFor(getSleepTimeMs(retryCount, elapsedTimeMs), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                // 被中断 - 表示用户期望退出
                Thread.currentThread().interrupt();
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * 计算本次睡眠的时间
     * @param retryCount 这是第几次重试 - 首次重试时该值为0
     * @param elapsedTimeMs 操作开始到现在已过去的时间
     * @return 本次睡眠时间
     */
    protected abstract long getSleepTimeMs(int retryCount, long elapsedTimeMs);
}
