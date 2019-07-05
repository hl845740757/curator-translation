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
import org.apache.curator.framework.state.ConnectionState;
import java.util.List;

/**
 * 缓存事件。
 *
 * Q:为什么事件对象中没有{@link PathChildrenCache} 呢，或者说事件对象为什么有{@link ChildData}呢？
 *
 * A:{@link PathChildrenCache}中的数据改变是由 main-EventThread执行的，是最新的数据。
 * 更新数据之后，会将事件处理操作提交给事件处理线程（避免阻塞main-EventThread线程）。
 * 而事件处理者是另一个线程，无法保证事件与{@link PathChildrenCache}中数据的一致性（异步过程）。
 * 导致：事件可能是旧的事件，而{@link PathChildrenCache}中却是最新的数据，因此事件处理器一定不能从{@link PathChildrenCache}获取数据。
 * 事件处理器线程只能通过事件中的数据一步一步更新自己的本地缓存，以最终和main-EventThread线程中的Cache达成一致(最终一致性)。
 *
 * 举个栗子：事件处理器正在处理一个{@link Type#CHILD_ADDED}事件，如果从{@link PathChildrenCache}中获取节点数据，则可能出现异常，
 * 因为调度队列可能还存在一堆待处理的事件。
 *
 * POJO that abstracts a change to a path
 */
public class PathChildrenCacheEvent
{
    /**
     * 事件类型
     */
    private final Type type;

    /**
     * 该事件对于的数据，如果是子节点事件，该字段不为null。（节点移除时间返回的删除前的数据）
     */
    private final ChildData data;

    /**
     * Type of change.
     * 事件的类型，关于子节点事件，事件的产生是与缓存比较得出的。
     */
    public enum Type
    {
        /**
         * A child was added to the path.
         * 当拉取到的节点不在现有缓存中时，产生该事件。
         */
        CHILD_ADDED,

        /**
         * A child's data was changed.
         * 当节点的最新状态与缓存状态不同(版本号不同)时，产生该事件。
         */
        CHILD_UPDATED,

        /**
         * A child was removed from the path.
         * 当缓存的节点不在父节点的最新孩子节点中时，产生该事件。
         */
        CHILD_REMOVED,

        /**
         * Called when the connection has changed to {@link ConnectionState#SUSPENDED}
         *
         * This is exposed so that users of the class can be notified of issues that *might* affect normal operation.
         * The PathChildrenCache is written such that listeners are not expected to do anything special on this
         * event, except for those people who want to cause some application-specific logic to fire when this occurs.
         * While the connection is down, the PathChildrenCache will continue to have its state from before it lost
         * the connection and after the connection is restored, the PathChildrenCache will emit normal child events
         * for all of the adds, deletes and updates that happened during the time that it was disconnected.
         */
        CONNECTION_SUSPENDED,

        /**
         * Called when the connection has changed to {@link ConnectionState#RECONNECTED}
         *
         * This is exposed so that users of the class can be notified of issues that *might* affect normal operation.
         * The PathChildrenCache is written such that listeners are not expected to do anything special on this
         * event, except for those people who want to cause some application-specific logic to fire when this occurs.
         * While the connection is down, the PathChildrenCache will continue to have its state from before it lost
         * the connection and after the connection is restored, the PathChildrenCache will emit normal child events
         * for all of the adds, deletes and updates that happened during the time that it was disconnected.
         */
        CONNECTION_RECONNECTED,

        /**
         * Called when the connection has changed to {@link ConnectionState#LOST}
         *
         * This is exposed so that users of the class can be notified of issues that *might* affect normal operation.
         * The PathChildrenCache is written such that listeners are not expected to do anything special on this
         * event, except for those people who want to cause some application-specific logic to fire when this occurs.
         * While the connection is down, the PathChildrenCache will continue to have its state from before it lost
         * the connection and after the connection is restored, the PathChildrenCache will emit normal child events
         * for all of the adds, deletes and updates that happened during the time that it was disconnected.
         */
        CONNECTION_LOST,

        /**
         * 当以{@link PathChildrenCache.StartMode#POST_INITIALIZED_EVENT}模式启动{@link PathChildrenCache}后，
         * 当第一次拉取完所有子节点的数据时，会提交一次该事件，表示缓存数据已初始化好（可以方便的获取子节点的初始数据）。
         * {@link #getInitialData()}
         *
         * Posted when {@link PathChildrenCache#start(PathChildrenCache.StartMode)} is called
         * with {@link PathChildrenCache.StartMode#POST_INITIALIZED_EVENT}. This
         * event signals that the initial cache has been populated.
         */
        INITIALIZED
    }

    /**
     * @param type event type
     * @param data event data or null
     */
    public PathChildrenCacheEvent(Type type, ChildData data)
    {
        this.type = type;
        this.data = data;
    }

    /**
     * @return change type
     */
    public Type getType()
    {
        return type;
    }

    /**
     * @return the node's data
     */
    public ChildData getData()
    {
        return data;
    }

    /**
     * 特殊用途的方法，当收到{@link Type#INITIALIZED}事件时，可以调用该方法接收缓存的初始状态(初始数据)。
     *
     * Special purpose method. When an {@link Type#INITIALIZED}
     * event is received, you can call this method to
     * receive the initial state of the cache.
     *
     * @return initial state of cache for {@link Type#INITIALIZED} events. Otherwise, <code>null</code>.
     */
    public List<ChildData> getInitialData()
    {
        return null;
    }

    @Override
    public String toString()
    {
        return "PathChildrenCacheEvent{" +
            "type=" + type +
            ", data=" + data +
            '}';
    }
}
