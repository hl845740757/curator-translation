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
package org.apache.curator.framework.recipes.cache;

import org.apache.curator.framework.CuratorFramework;

/**
 * {@link PathChildrenCache}事件的监听器，当{@link PathChildrenCache}的状态发生改变时，监听器将会收到通知。
 *
 * Listener for PathChildrenCache changes
 */
public interface PathChildrenCacheListener
{
    /**
     * 当事件产生时该方法将会被调用。
     * 再次注意：这里是异步调用。事件可能是旧事件，而{@link PathChildrenCache}中的数据是新数据。
     *
     * Called when a change has occurred
     *
     * @param client the client curator客户端
     * @param event describes the change 事件对应的数据
     * @throws Exception errors
     */
    public void     childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception;
}
