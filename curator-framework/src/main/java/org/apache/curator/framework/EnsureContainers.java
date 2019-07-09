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
package org.apache.curator.framework;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 确保容器路径存在的类，当容器节点不存在的时候，创建容器节点（通过创建一个子节点"foo"来创建容器）。
 *
 * Similar to {@link org.apache.curator.utils.EnsurePath} but creates containers.
 *
 */
public class EnsureContainers
{
    private final CuratorFramework client;
    private final String path;

    /**
     * 是否需要执行。
     * Q: 为何需要原子变量呢？
     * A: synchronize + Atomic/volatile 实现并发下的高效读，提高查询效率。
     *    高效读：只读操作不需要获得锁，不需要互斥执行。而写操作，需要互斥执行，需要获得锁。
     *    synchronize用于阻塞其它线程，Atomic用于保证只执行一次。
     */
    private final AtomicBoolean ensureNeeded = new AtomicBoolean(true);

    /**
     * @param client the client
     * @param path path to ensure is containers
     */
    public EnsureContainers(CuratorFramework client, String path)
    {
        this.client = client;
        this.path = path;
    }

    /**
     * 第一次调用该方法的时候，如果需要的话，该路径上的所有节点将会被创建为容器。
     *
     * The first time this method is called, all nodes in the
     * path will be created as containers if needed
     *
     * @throws Exception errors
     */
    public void ensure() throws Exception
    {
        // 如果检测到了，已经执行了保证操作，则直接返回，无需竞争锁
        if ( ensureNeeded.get() )
        {
            // 需要执行一次保证操作
            internalEnsure();
        }
    }

    /**
     * Q: 该方法为何需要加锁呢？
     * A: 该方法需要互斥执行，后续线程需要等待前面的线程创建节点成功之后才能返回！
     * @throws Exception zookeeper errors
     */
    private synchronized void internalEnsure() throws Exception
    {
        // 想达到的目的：通过原子变量保证只执行一次。
        if ( ensureNeeded.compareAndSet(true, false) )
        {
            // 这个好像有bug啊，这样的写法导致了锁是没有意义的，因为检测标记不需要获得锁。
            // eg: 如果线程在这里突然被操作系统挂起了，其它线程在ensure那里检测到标记为false，则直接返回了。
            // 此时，其它线程以为已经执行了 createContainers操作，实际还没有！！！
            // ensureNeeded的更新应该放在 createContainers操作 之后才能保证看见标记为false的时候，节点一定已经创建了！
            client.createContainers(path);
        }

        // wjybxx: 我觉得应该是这样的 double check
//        if (ensureNeeded.get()){
//            client.createContainers(path);
//            ensureNeeded.set(false);
//        }
    }
}
