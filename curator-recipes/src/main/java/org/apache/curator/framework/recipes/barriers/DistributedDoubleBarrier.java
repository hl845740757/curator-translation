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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.utils.PathUtils;

/**
 * 分布式双重屏障；（该类还有部分内容没想的特别明白，暂时先放一下，卡在这不好）
 *
 * 这是一个如zookeeper食谱中描述的双重屏障，描述如下：
 * <blockquote>
 *      双屏障使客户机能够同步计算的开始和结束。当足够多的进程加入屏障后，进程开始计算，并在完成后离开屏障。
 * </blockquote>
 *
 * 初步猜测一下实现？
 * 初步感觉可能类似{@link java.util.concurrent.CyclicBarrier}，认为可以这样实现：
 * 开始时，先创建一个临时节点，等待出现足够多的开始节点，当开始节点到达一定数量的时候，开始执行。
 * 计算完成时候，再创建一个临时节点，等待出现足够多的结束节点，当结束节点到达一定数量的时候，完成。
 * （或者设置两个数，每个线程开始或完成的时候都将数字减一，就像向{@link java.util.concurrent.CyclicBarrier}了）
 *
 * 实际实现：
 *
 *
 * 注意：不可以多个线程在该屏障上等待（因为节点只能创建一次），也就是说该对象不可以多线程共享。
 *
 * 状态迁移：
 * 创建ourPath对应的节点 ----> 准备执行状态
 * ready节点出现        ---->  执行状态
 * 删除outPath对应节点  -----> 准备离开状态
 * 所有ourPath节点都删除 ----->  离开状态
 *
 * <p>
 *     A double barrier as described in the ZK recipes. Quoting the recipe:
 * </p>
 *
 * <blockquote>
 *     Double barriers enable
 *     clients to synchronize the beginning and the end of a computation. When enough processes
 *     have joined the barrier, processes start their computation and leave the barrier
 *     once they have finished.
 * </blockquote>
 */
public class DistributedDoubleBarrier
{
    private final CuratorFramework client;

    /** 屏障路径 */
    private final String barrierPath;
    /**
     * 屏障中的成员数。
     * 注意：可以有超过成员数的线程/进程进入屏障。成员数是一个阈值，而不是一个限制(上限)。
     */
    private final int memberQty;

    /**
     * 表示该对象的节点路径。
     * 该path是由UUID创建的，每个对象的path会不一致。
     * {@link #enter()} 与 {@link #leave()}与该节点的增加和删除有关，因此该对象是不能复用的。
     * （节点不可以重复创建与删除）
     */
    private final String ourPath;

    /**
     * 屏障中表示大家已经准备好的节点。
     * 为什么需要一个标记？
     * 1. 当检查到该标记的时候，那么证明可通过开始屏障，表示屏障进入了计算阶段。
     * 2. 客户端需要观察该节点的创建，以从阻塞中醒来。
     * 3. 程序通过屏障以后，可能会删除自身的节点{@link #ourPath}，后面进入的程序也仍然可以通过。
     * 4. 节点数大于需求数可以证明已经打开屏障，但节点数小于需求数不能证明屏障未打开。
     */
    private final String readyPath;

    /** 是否已经进行了通知，屏障是否已经打开 */
    private final AtomicBoolean hasBeenNotified = new AtomicBoolean(false);
    /** 客户端是否断开了连接 */
    private final AtomicBoolean connectionLost = new AtomicBoolean(false);

    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            // 更新链接状态
            connectionLost.set(event.getState() != Event.KeeperState.SyncConnected);
            // 唤醒在该对象上等待的线程，最多只有一个线程
            notifyFromWatcher();
        }
    };

    private static final String     READY_NODE = "ready";

    /**
     * Creates the barrier abstraction. <code>memberQty</code> is the number of members in the
     * barrier. When {@link #enter()} is called, it blocks until all members have entered. When
     * {@link #leave()} is called, it blocks until all members have left.
     *
     * @param client the client
     * @param barrierPath path to use 屏障使用的路径
     * @param memberQty the number of members in the barrier. NOTE: more than <code>memberQty</code>
     *                  can enter the barrier. <code>memberQty</code> is a threshold, not a limit.
     *                  屏障中的成员数。注意：可以有超过成员数的线程/进程进入屏障。成员数是一个阈值，而不是一个限制(上限)。
     */
    public DistributedDoubleBarrier(CuratorFramework client, String barrierPath, int memberQty)
    {
        Preconditions.checkState(memberQty > 0, "memberQty cannot be 0");

        this.client = client;
        this.barrierPath = PathUtils.validatePath(barrierPath);
        this.memberQty = memberQty;
        ourPath = ZKPaths.makePath(barrierPath, UUID.randomUUID().toString());
        readyPath = ZKPaths.makePath(barrierPath, READY_NODE);
    }

    /**
     * 进入屏障并且阻塞到所有的成员进入了该屏障。
     * （从 准备执行 阻塞到 可执行状态）
     *
     * Enter the barrier and block until all members have entered
     *
     * @throws Exception interruptions, errors, etc.
     */
    public void     enter() throws Exception
    {
        enter(-1, null);
    }

    /**
     * 进入屏障并且阻塞到所有的成员进入了该屏障 或 阻塞超时。
     * （尝试在有限的时间内从 准备执行 阻塞到 可执行状态）
     *
     * Enter the barrier and block until all members have entered or the timeout has
     * elapsed
     *
     * @param maxWait max time to block 最长阻塞时间
     * @param unit time unit 时间单位
     * @return true if the entry was successful, false if the timeout elapsed first
     *          进入屏障成功时返回true，超时则返回false。 -> 返回结果表示是否可以进行下一步操作
     * @throws Exception interruptions, errors, etc.
     */
    public boolean     enter(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        // ready子节点是否已经存在？ 是否已经允许进入执行状态
        boolean         readyPathExists = (client.checkExists().usingWatcher(watcher).forPath(readyPath) != null);
        // 创建表示该对象的子节点。节点不可以反复创建 => 不可以多线程同时使用该对象进入屏障
        // 如果不是临时节点会导致其它节点无法离开屏障，导致死锁
        client.create().creatingParentContainersIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ourPath);

        // ready节点已存在，或尝试进入屏障成功，则表示需要的成员都已进入，屏障已打开（可以进入到执行状态）
        boolean         result = (readyPathExists || internalEnter(startMs, hasMaxWait, maxWaitMs));
        if ( connectionLost.get() )
        {
            // 发现连接已断开，抛出异常
            throw new KeeperException.ConnectionLossException();
        }
        // 返回通过屏障结果
        return result;
    }

    /**
     * 离开屏障并阻塞到所有的成员离开屏障。
     * （退出屏障）
     * Leave the barrier and block until all members have left
     *
     * @throws Exception interruptions, errors, etc.
     */
    public synchronized void     leave() throws Exception
    {
        leave(-1, null);
    }

    /**
     * 离开屏障并阻塞到所有的成员离开屏障 或 超时。
     * （尝试在有限时间内退出屏障）
     *
     * Leave the barrier and block until all members have left or the timeout has
     * elapsed
     *
     * @param maxWait max time to block 最长阻塞时间
     * @param unit time unit 时间单位
     * @return true if leaving was successful, false if the timeout elapsed first
     *          离开屏障成功时返回true，超时则返回false。
     * @throws Exception interruptions, errors, etc.
     */
    public synchronized boolean     leave(long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;

        return internalLeave(startMs, hasMaxWait, maxWaitMs);
    }

    /**
     * 获取屏障下的所有子节点。
     * 屏障节点下都有什么呢？ 可能有一个ready节点，可能还有多个UUID构成的节点。
     *
     * @return children
     * @throws Exception zookeeper errors
     */
    @VisibleForTesting
    protected List<String> getChildrenForEntering() throws Exception
    {
        return client.getChildren().forPath(barrierPath);
    }

    private List<String> filterAndSortChildren(List<String> children)
    {
        // 过滤掉children中的ready节点
        Iterable<String> filtered = Iterables.filter
        (
            children,
            new Predicate<String>()
            {
                @Override
                public boolean apply(String name)
                {
                    return !name.equals(READY_NODE);
                }
            }
        );
        // 再对过滤后的子节点排序
        ArrayList<String> filteredList = Lists.newArrayList(filtered);
        Collections.sort(filteredList);
        return filteredList;
    }

    /**
     * 退出屏障的真正实现
     * @param startMs 请求离开屏障时的时间戳
     * @param hasMaxWait 是否有限的等待
     * @param maxWaitMs 如果有限的等待，最大等待时间
     * @return 离开屏障成功则返回true，否则返回false
     * @throws Exception zookeeper errors
     */
    private boolean internalLeave(long startMs, boolean hasMaxWait, long maxWaitMs) throws Exception
    {
        // 获取自身的节点名字(去掉父节点路径)
        String          ourPathName = ZKPaths.getNodeFromPath(ourPath);
        // 我的节点是否存在（如果为true，表示需要删除自身节点，进入准备离开状态）
        boolean         ourNodeShouldExist = true;
        boolean         result = true;
        for(;;)
        {
            // 在每次尝试之前，检查是否断开了连接，如果断开了连接，抛出异常
            if ( connectionLost.get() )
            {
                throw new KeeperException.ConnectionLossException();
            }

            // 获取屏障下的所有子节点，如果该屏障节点尚未创建，则使用空列表
            List<String> children;
            try
            {
                children = client.getChildren().forPath(barrierPath);
            }
            catch ( KeeperException.NoNodeException dummy )
            {
                children = Lists.newArrayList();
            }
            // 去除掉ready节点，并对节点进行排序
            children = filterAndSortChildren(children);
            // children 应该是不会为null的，如果该屏障下没有ready以外的子节点了，
            // 证明所有的节点已准备好离开了，这时大家真正的一起离开屏障
            if ( (children == null) || (children.size() == 0) )
            {
                break;
            }

            int                 ourIndex = children.indexOf(ourPathName);
            if ( (ourIndex < 0) && ourNodeShouldExist )
            {
                // 没有找到自身的节点，而还删除自身对应的节点
                if ( connectionLost.get() )
                {
                    // 如果断开了连接，可能导致临时节点删除
                    break;  // connection was lost but we've reconnected. However, our ephemeral node is gone
                }
                else
                {
                    // 连接状态下，临时节点不存在，那么表明可能没有调用 enter，必须在该屏障上进行等待，才能离开屏障
                    throw new IllegalStateException(String.format("Our path (%s) is missing", ourPathName));
                }
            }
            // 到这里，要么 ourIndex >= 0 要么 ourNodeShouldExist 为 false

            if ( children.size() == 1 )
            {
                if ( ourNodeShouldExist && !children.get(0).equals(ourPathName) )
                {
                    // 如果还未删除自身，但仅剩下一个节点，如果不是自己，那么状态异常
                    throw new IllegalStateException(String.format("Last path (%s) is not ours (%s)", children.get(0), ourPathName));
                }
                // 如果我是唯一的那个节点，表示是我在等待其它节点删除，且其它节点都已删除，我可以删除自身了
                // 如果我不是唯一的那个节点，则等待它的删除
                checkDeleteOurPath(ourNodeShouldExist);
                break;
            }

            // 到这里 ourNodeShouldExist 和 children是否存在自身一致

            Stat            stat;
            boolean         IsLowestNode = (ourIndex == 0);
            if ( IsLowestNode )
            {
                // 当前对象对应的节点如果是第一个（字符串最小）的那个节点（和顺序节点不一样），
                // 那么它等待最大的那个节点的删除。由于是循环执行，也就是说理想状态下，它会监听到除自己之外其他所有节点的删除。
                // 而其它客户端只会监听一个节点的删除时间，
                // 可以极大的减少广播量！事件通知数随着节点数的增加是线性增加的 2 * (n-1)
                String  highestNodePath = ZKPaths.makePath(barrierPath, children.get(children.size() - 1));
                stat = client.checkExists().usingWatcher(watcher).forPath(highestNodePath);
            }
            else
            {
                // 否则都观察最小的那个节点的删除，其它节点的删除都不需要关注，配合上面的代码可以极大的减少事件数。
                String  lowestNodePath = ZKPaths.makePath(barrierPath, children.get(0));
                stat = client.checkExists().usingWatcher(watcher).forPath(lowestNodePath);

                checkDeleteOurPath(ourNodeShouldExist);
                ourNodeShouldExist = false;
            }

            if ( stat != null )
            {
                // 我观察的节点尚存在
                if ( hasMaxWait )
                {
                    long        elapsed = System.currentTimeMillis() - startMs;
                    long        thisWaitMs = maxWaitMs - elapsed;
                    if ( thisWaitMs <= 0 )
                    {
                        // 剩余等待时间不足，超时返回失败
                        result = false;
                    }
                    else
                    {
                        // 继续等待，挂起自身，等待事件线程唤醒自己，唤醒后再次尝试
                        wait(thisWaitMs);
                    }
                }
                else
                {
                    // 无时限等待，等待事件线程唤醒自己，唤醒后再次尝试
                    wait();
                }
            }
        }

        // 删除ready节点
        // 退出失败也需要删除吗？ 主要考虑的问题是： 一部分节点已经开始离开了，同时又有节点进入的时候
        // 新进入的节点可能能参与到这一波中，也可能需要等待下一波，当前波的节点在离开时会删除ready，而新加入的节点又在等待ready
        // 最小的节点会等待所有的节点删除，新进入的由于无法执行，会一直等待
        try
        {
            client.delete().forPath(readyPath);
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore 可以忽略该异常，有其它节点删除了ready节点，冲突可能是有害的，也可能是无害的
        }

        return result;
    }

    /**
     * 在尝试退出屏障时，检查是否删除{@link #ourPath}对应的节点（该对象对应的节点）
     * @param shouldExist ourPath对应的节点是否应该存在
     * @throws Exception zookeeper errors
     */
    private void checkDeleteOurPath(boolean shouldExist) throws Exception
    {
        if ( shouldExist )
        {
            client.delete().forPath(ourPath);
        }
    }

    /**
     * 在屏障上等待的内部实现
     * @param startMs 进入时间
     * @param hasMaxWait 是有为等待时间限制
     * @param maxWaitMs 当有等待时间限制的时候，最大等待时间
     * @return
     * @throws Exception zookeeper errors
     */
    private synchronized boolean internalEnter(long startMs, boolean hasMaxWait, long maxWaitMs) throws Exception
    {
        boolean result = true;
        do
        {
            List<String>    children = getChildrenForEntering();
            int             count = (children != null) ? children.size() : 0;
            if ( count >= memberQty )
            {
                // 进入屏障的节点数已经满足期望了，创建ready节点，表示所有节点可以进入下一步骤
                try
                {
                    client.create().forPath(readyPath);
                }
                catch ( KeeperException.NodeExistsException ignore )
                {
                    // ignore 有其它客户端创建了ready节点，可以忽略该异常。
                    // 因为不论谁看见节点数满足条件，都是真正的满足条件了，谁创建ready标记都是可以的。
                }
                break;
            }

            if ( hasMaxWait && !hasBeenNotified.get() )
            {
                long        elapsed = System.currentTimeMillis() - startMs;
                long        thisWaitMs = maxWaitMs - elapsed;
                if ( thisWaitMs <= 0 )
                {
                    result = false;
                }
                else
                {
                    wait(thisWaitMs);
                }

                if ( !hasBeenNotified.get() )
                {
                    result = false;
                }
            }
            else
            {
                wait();
            }
        } while ( false );
        // while (false) ??? 只执行一遍？

        return result;
    }

    private synchronized void notifyFromWatcher()
    {
        hasBeenNotified.set(true);
        notifyAll();
    }
}
