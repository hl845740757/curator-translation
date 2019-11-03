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

/**
 * 事件操作，用于延迟执行事件回调，减少main-EventThread的压力。
 */
class EventOperation implements Operation
{
    /**
     * 操作(命令)的接收者，真正执行逻辑的对象。
     */
    private final PathChildrenCache cache;
    /**
     * 事件相关数据
     */
    private final PathChildrenCacheEvent event;

    EventOperation(PathChildrenCache cache, PathChildrenCacheEvent event)
    {
        this.cache = cache;
        this.event = event;
    }

    /**
     * {@code PathChildrenCache#executorService}事件处理线程调用。
     * 通知事件的监听者们产生了一个事件。
     */
    @Override
    public void invoke()
    {
        cache.callListeners(event);
    }

    @Override
    public String toString()
    {
        return "EventOperation{" +
            "event=" + event +
            '}';
    }
}
