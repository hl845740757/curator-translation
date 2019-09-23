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

import java.io.Closeable;
import java.io.IOException;

/**
 * 租约(一个资源句柄)。
 * 它表示一个从{@link InterProcessSemaphoreV2}申请到的租约（资源）。
 * 当不再需要该租约的时候关闭该租约是客户端的职责，以使得其它阻塞的客户端能够使用它。
 * 如果客户端崩溃或者session超时，那么该租约将会主动被关闭 - （可以猜测存在临时节点，而且每个租约一个临时节点）。
 *
 * Represents an acquired lease from an {@link InterProcessSemaphoreV2}. It is the client's responsibility
 * to close this lease when it is no longer needed so that other blocked clients can use it. If the
 * client crashes (or its session expires, etc.) the lease will automatically be closed.
 */
public interface Lease extends Closeable
{
    /**
     * 释放该租约，以便其它客户端/进程能够获得它。
     *
     * Releases the lease so that other clients/processes can acquire it
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException;

    /**
     * 获取存放在该租约上的数据 - 应该是申请租约/资源时指定的数据。
     *
     * Return the data stored in the node for this lease
     *
     * @return data
     * @throws Exception errors
     */
    public byte[]   getData() throws Exception;

    /**
     * 获取该租约节点的名字。 NodeName也可以立即为simpleName
     *
     * Return the the node for this lease
     *
     * @return data
     * @throws Exception errors
     */
    public String getNodeName();
}
