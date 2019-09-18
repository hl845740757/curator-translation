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

/**
 * 锁撤销操作监听器
 * @param <T>
 */
public interface RevocationListener<T>
{
    /**
     * 请求撤销锁(请求释放锁)。
     * 当收到一个撤销锁的请求时，该方法会被调用。当收到该请求时，你应该尽快释放锁。
     *
     * 注意：撤销是一种协作机制。
     * 它意味着：不能强制对方释放锁，只是发起一个请求，由锁的拥有者决定是否响应请求以及何时响应请求 (可以参考中断)。
     *
     * Called when a revocation request has been received. You should release the lock as soon
     * as possible. Revocation is cooperative.
     *
     * @param forLock the lock that should release
     */
    public void         revocationRequested(T forLock);
}
