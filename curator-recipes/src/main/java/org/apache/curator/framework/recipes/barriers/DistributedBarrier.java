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
package org.apache.curator.framework.recipes.barriers;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import java.util.concurrent.TimeUnit;
import org.apache.curator.utils.PathUtils;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * 分布式屏障（该屏障可以共享）；
 *
 * 这是一个如zookeeper食谱中描述的屏障：
 * <blockquote>
 *     分布式系统使用屏障阻塞一组节点继续执行，直到满足一个（特定的）条件，此时所有阻塞的节点都可以继续执行。
 * </blockquote>
 *
 * 可以初次设想一下它的实现方式？
 * 某个特殊节点（不）存在时进行等待，直到目标节点删除（创建）后恢复执行，
 * 应该和JVM的{@link java.util.concurrent.CountDownLatch}类似。
 *
 * 实际的实现方式：
 * 节点存在，屏障存在，节点消失，屏障消失。
 *
 * 这样的设计有一个限制：屏障节点是不能存放数据的。
 * 不过这只是一个很小的限制，可以在另一个节点存放数据，并由该屏障保护即可。
 *
 * <h3>为什么方法都是同步的？</h3>
 * 1. 因为使用了等待通知机制(条件队列)。
 *    通知线程是main-EventThread线程，需要唤醒在该对象上等待的线程，必须要先持有锁。
 *    使用该屏障的线程必须先获得锁，才可以检查状态，然后在该对象上等待。
 * 2. 可以降低远程的竞争程度。（先在本地进行竞争，然后才能在远程竞争）（这只是附加效果）
 *
 * 注意：该对象可以多线程共享，可以多个线程在该对象的条件队列上等待。
 *
 * <p>
 *     A barrier as described in the ZK recipes. Quoting the recipe:
 * </p>
 *
 * <blockquote>
 *     Distributed systems use barriers to block processing of a set of nodes
 *     until a condition is met at which time all the nodes are allowed to proceed
 * </blockquote>
 */
public class DistributedBarrier
{
    /** curator客户端 */
    private final CuratorFramework client;
    /** 屏障节点路径 */
    private final String barrierPath;
    /**
     * 屏障节点的watcher，监听节点事件（创建、删除、更新）
     * 有效的事件只有节点删除事件{@link EventType#NodeDeleted}
     */
    private final Watcher watcher = new Watcher()
    {
        /**
         * 处理事件
         * @param event 节点删除或节点创建事件
         */
        @Override
        public void process(WatchedEvent event)
        {
            client.postSafeNotify(DistributedBarrier.this);
        }
    };

    /**
     * @param client client curator客户端
     * @param barrierPath path to use as the barrier 作为屏障的节点
     */
    public DistributedBarrier(CuratorFramework client, String barrierPath)
    {
        this.client = client;
        this.barrierPath = PathUtils.validatePath(barrierPath);
    }

    /**
     * 创建屏障。
     * 它的实现是创建节点，也就是说节点存在表示屏障存在，节点删除，屏障消失。
     *
     * Utility to set the barrier node
     *
     * @throws Exception errors
     */
    public synchronized void         setBarrier() throws Exception
    {
        try
        {
            client.create().creatingParentContainersIfNeeded().forPath(barrierPath);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // ignore 屏障（节点）已经被其它客户端创建了，可以忽略该异常。
            // 因为该节点是我创建的还是他人创建的并不重要，只要节点存在即可。（并发冲突并不都是有害的）
        }
    }

    /**
     * 删除屏障。
     * 它的实现是删除节点，也就是说节点存在表示屏障存在，节点删除，屏障消失。
     *
     * Utility to remove the barrier node
     *
     * @throws Exception errors
     */
    public synchronized void      removeBarrier() throws Exception
    {
        try
        {
            client.delete().forPath(barrierPath);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore 屏障（节点）已经被其它客户端删除了，可以忽略该异常。
            // 我的目的是删除屏障，至于是我删除的其它客户端删除的并不重要。
        }
    }

    /**
     * 在屏障上等待，也就是watch。  直到节点删除事件产生。
     *
     * Blocks until the barrier node comes into existence
     *
     * @throws Exception errors
     */
    public synchronized void      waitOnBarrier() throws Exception
    {
        waitOnBarrier(-1, null);
    }

    /**
     * Blocks until the barrier no longer exists or the timeout elapses
     *
     * @param maxWait max time to block
     * @param unit time unit
     * @return true if the wait was successful, false if the timeout elapsed first
     * @throws Exception errors
     */
    public synchronized boolean      waitOnBarrier(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        boolean         result;
        for(;;)
        {
            // 检查节点是否存在，同时注册watcher。
            // 如果当前节点不存在，当节点被创建时，会收到通知
            // 如果当前节点存在，当节点被删除时，会收到通知
            result = (client.checkExists().usingWatcher(watcher).forPath(barrierPath) == null);
            if ( result )
            {
                // 节点不存在，表示没有屏障，等待结束。跳出循环。
                break;
            }

            // 节点存在，需要等待watcher的节点删除事件
            // 等待又分为有时限的等待，和无时限的等待
            if ( hasMaxWait )
            {
                long        elapsed = System.currentTimeMillis() - startMs;
                long        thisWaitMs = maxWaitMs - elapsed;
                if ( thisWaitMs <= 0 )
                {
                    break;
                }
                wait(thisWaitMs);
            }
            else
            {
                wait();
            }
        }
        return result;
    }
}
