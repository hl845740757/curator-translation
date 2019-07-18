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

import org.apache.curator.framework.CuratorFramework;
import java.util.List;

/**
 * 互斥锁驱动
 */
public interface LockInternalsDriver extends LockInternalsSorter
{
    /**
     * 根据节点的最新子节点信息尝试获取锁。
     * @param client curator客户端
     * @param children 该锁路径下的所有子节点。
     * @param sequenceNodeName 该线程对应的有序节点名字
     * @param maxLeases 直译：最大租约。 解释：指最多允许几个线程(不一定属于同一进程)同时获得锁。
     * @return 操作结果，是否获得锁，如果未获得锁，应该监听哪个节点。
     * @throws Exception zookeeper errors
     */
    public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception;

    /**
     * 为当前线程在锁路径下创建一个对应的子节点。
     * @param client curator客户端
     * @param path 互斥锁路径
     * @param lockNodeBytes 子节点前缀
     * @return ourPath 临时顺序节点的完整路径
     * @throws Exception zookeeper errors
     */
    public String createsTheLock(CuratorFramework client,  String path, byte[] lockNodeBytes) throws Exception;
}
