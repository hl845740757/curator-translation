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
 * {@link TreeCache}事件处理监听器。
 *
 * Listener for {@link TreeCache} changes
 */
public interface TreeCacheListener
{
    /**
     * {@link TreeCache}的事件回调。
     * 与相同的警告：{@link PathChildrenCacheListener#childEvent(CuratorFramework, PathChildrenCacheEvent)}
     * 该事件的处理线程是独立的线程，与更新{@link TreeCache}数据的线程是两个线程，处理事件时最好不要使用{@link TreeCache}。
     *
     * 更新数据的线程是 main-EventThread,处理事件的线程是TreeCache创建的一个单线程的Executor。
     *
     * Called when a change has occurred
     *
     * @param client the client
     * @param event  describes the change
     * @throws Exception errors
     */
    public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception;
}
