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
package org.apache.curator;

/**
 * 重试策略 - 当一个操作失败时的重试机制。
 *
 * Abstracts the policy to use when retrying connections
 */
public interface RetryPolicy
{
    /**
     * Called when an operation has failed for some reason. This method should return
     * true to make another attempt.
     *
     *
     * @param retryCount the number of times retried so far (0 the first time)
     *                这是第几次重试 - 首次重试时该值为0
     * @param elapsedTimeMs the elapsed time in ms since the operation was attempted
     *                操作开始到现在过去的总时间(已消耗的总时间)
     * @param sleeper use this to sleep - DO NOT call Thread.sleep
     *                用于等待的方式 - 注意：不要调用{@link Thread#sleep(long)}。
     *                比起调用{@link Thread#sleep(long)}有更多的灵活性。
     * @return true/false
     */
    public boolean      allowRetry(int retryCount, long elapsedTimeMs, RetrySleeper sleeper);
}
