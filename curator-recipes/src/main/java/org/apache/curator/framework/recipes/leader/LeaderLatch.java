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

package org.apache.curator.framework.recipes.leader;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.AfterConnectionEstablished;
import org.apache.curator.framework.recipes.locks.LockInternals;
import org.apache.curator.framework.recipes.locks.LockInternalsSorter;
import org.apache.curator.framework.recipes.locks.StandardLockInternalsDriver;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.curator.utils.PathUtils;

/**
 * 选举锁。
 * 在连接到ZooKeeper集群的一组JMV中的多个竞争者中选择“leader”的抽象。
 * 如果一个由n个线程/进程组成的组争夺领导权，则将随机指派一个线程/进程作为领导，
 * 直到它释放领导权，此时将随机从该组中选择另一个线程/进程作为leader。 -- 本质是抢占节点，创建一个临时节点，节点过期则重新选举
 * <p>
 * 这个类的复杂度有点超过了我的预期... 我觉得这个复杂度的提升主要来自于连接状态管理。
 *
 * <p>
 * Abstraction to select a "leader" amongst multiple contenders in a group of JMVs connected to
 * a Zookeeper cluster. If a group of N thread/processes contend for leadership one will
 * randomly be assigned leader until it releases leadership at which time another one from the
 * group will randomly be chosen
 * </p>
 */
public class LeaderLatch implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    /**
     * 锁路径
     */
    private final String latchPath;
    /**
     * 我作为参与者的id
     */
    private final String id;
    /**
     * 锁的状态
     */
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    /**
     * 当前是否是leader - 这是一个缓存值
     */
    private final AtomicBoolean hasLeadership = new AtomicBoolean(false);
    /**
     * 作为参与者的路径 - 临时顺序节点，在放弃leader的时候回重创建
     */
    private final AtomicReference<String> ourPath = new AtomicReference<String>();
    /**
     * 监听器容器。
     */
    private final ListenerContainer<LeaderLatchListener> listeners = new ListenerContainer<LeaderLatchListener>();
    /**
     * 关闭模式：关闭锁时是否通知监听器，默认不通知。
     */
    private final CloseMode closeMode;
    /**
     * 启动时注册的任务。
     * 其实它这里没有太大必要使用{@link AtomicReference}
     */
    private final AtomicReference<Future<?>> startTask = new AtomicReference<Future<?>>();

    /**
     * 监听连接状态
     */
    private final ConnectionStateListener listener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };

    /**
     * 锁名字 -- 锁路径下顺序节点的前缀
     */
    private static final String LOCK_NAME = "latch-";

    /**
     * 锁节点排序器。
     * -> 计算临时顺序节点的序号。
     */
    private static final LockInternalsSorter sorter = new LockInternalsSorter()
    {
        @Override
        public String fixForSorting(String str, String lockName)
        {
            //
            return StandardLockInternalsDriver.standardFixForSorting(str, lockName);
        }
    };

    public enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * How to handle listeners when the latch is closed
     */
    public enum CloseMode
    {
        /**
         * 静悄悄的关闭 -> 当选举锁关闭时，不通知监听器。
         *
         * When the latch is closed, listeners will *not* be notified (default behavior)
         */
        SILENT,

        /**
         * 通知leader -> 当选举锁关闭时，如果当前是leader，那么在关闭时会通知监听器。
         *
         * When the latch is closed, listeners *will* be notified
         */
        NOTIFY_LEADER
    }

    /**
     * @param client    the client
     * @param latchPath the path for this leadership group
     */
    public LeaderLatch(CuratorFramework client, String latchPath)
    {
        this(client, latchPath, "", CloseMode.SILENT);
    }

    /**
     * @param client    the client
     * @param latchPath the path for this leadership group
     * @param id        participant ID
     */
    public LeaderLatch(CuratorFramework client, String latchPath, String id)
    {
        this(client, latchPath, id, CloseMode.SILENT);
    }

    /**
     * @param client    the client
     * @param latchPath the path for this leadership group
     *                  选举路径
     * @param id        participant ID
     *                  参与者id - 每一个参与者必须有一个唯一的id
     * @param closeMode behaviour of listener on explicit close.
     *                  关闭时触发的监听器动作
     */
    public LeaderLatch(CuratorFramework client, String latchPath, String id, CloseMode closeMode)
    {
        this.client = Preconditions.checkNotNull(client, "client cannot be null").newWatcherRemoveCuratorFramework();
        this.latchPath = PathUtils.validatePath(latchPath);
        this.id = Preconditions.checkNotNull(id, "id cannot be null");
        this.closeMode = Preconditions.checkNotNull(closeMode, "closeMode cannot be null");
    }

    /**
     * 把当前实例添加到leader选举中，并尝试申请领导权。
     *
     * Add this instance to the leadership election and attempt to acquire leadership.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        startTask.set(AfterConnectionEstablished.execute(client, new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    internalStart();
                }
                finally
                {
                    startTask.set(null);
                }
            }
        }));
    }

    /**
     * Remove this instance from the leadership election. If this instance is the leader, leadership
     * is released. IMPORTANT: the only way to release leadership is by calling close(). All LeaderLatch
     * instances must eventually be closed.
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        close(closeMode);
    }

    /**
     * Remove this instance from the leadership election. If this instance is the leader, leadership
     * is released. IMPORTANT: the only way to release leadership is by calling close(). All LeaderLatch
     * instances must eventually be closed.
     *
     * @param closeMode allows the default close mode to be overridden at the time the latch is closed.
     * @throws IOException errors
     */
    public synchronized void close(CloseMode closeMode) throws IOException
    {
        Preconditions.checkState(state.compareAndSet(State.STARTED, State.CLOSED), "Already closed or has not been started");
        Preconditions.checkNotNull(closeMode, "closeMode cannot be null");

        cancelStartTask();

        try
        {
            setNode(null);
            client.removeWatchers();
        }
        catch ( Exception e )
        {
            ThreadUtils.checkInterrupted(e);
            throw new IOException(e);
        }
        finally
        {
            client.getConnectionStateListenable().removeListener(listener);

            switch ( closeMode )
            {
            case NOTIFY_LEADER:
            {
                // 如果需要通知监听器 - 先更新状态（触发通知），再清空监听器。
                setLeadership(false);
                listeners.clear();
                break;
            }

            default:
            {
                // 如果不需要通知监听器 - 先清空监听器，再更新状态（不会触发通知）。
                listeners.clear();
                setLeadership(false);
                break;
            }
            }
        }
    }

    /**
     * 取消{@link #startTask}，通过future取消
     */
    @VisibleForTesting
    protected boolean cancelStartTask()
    {
        Future<?> localStartTask = startTask.getAndSet(null);
        if ( localStartTask != null )
        {
            localStartTask.cancel(true);
            return true;
        }
        return false;
    }

    /**
     * Attaches a listener to this LeaderLatch
     * <p>
     * Attaching the same listener multiple times is a noop from the second time on.
     * </p><p>
     * All methods for the listener are run using the provided Executor.  It is common to pass in a single-threaded
     * executor so that you can be certain that listener methods are called in sequence, but if you are fine with
     * them being called out of order you are welcome to use multiple threads.
     * </p>
     *
     * @param listener the listener to attach
     */
    public void addListener(LeaderLatchListener listener)
    {
        listeners.addListener(listener);
    }

    /**
     * Attaches a listener to this LeaderLatch
     * <p>
     * Attaching the same listener multiple times is a noop from the second time on.
     * </p><p>
     * All methods for the listener are run using the provided Executor.  It is common to pass in a single-threaded
     * executor so that you can be certain that listener methods are called in sequence, but if you are fine with
     * them being called out of order you are welcome to use multiple threads.
     * </p>
     *
     * @param listener the listener to attach
     * @param executor An executor to run the methods for the listener on.
     */
    public void addListener(LeaderLatchListener listener, Executor executor)
    {
        listeners.addListener(listener, executor);
    }

    /**
     * Removes a given listener from this LeaderLatch
     *
     * @param listener the listener to remove
     */
    public void removeListener(LeaderLatchListener listener)
    {
        listeners.removeListener(listener);
    }

    /**
     * 使当前线程阻塞等待当前实例成为leader，直到申请成功或被中断或当前实例被关闭。
     * 如果当前线程已经是leader，那么该方法将立即返回。
     *
     * <p>Causes the current thread to wait until this instance acquires leadership
     * unless the thread is {@linkplain Thread#interrupt interrupted} or {@linkplain #close() closed}.</p>
     * <p>If this instance already is the leader then this method returns immediately.</p>
     * <p></p>
     * <p>Otherwise the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:</p>
     * <ul>
     * <li>This instance becomes the leader</li>
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread</li>
     * <li>The instance is {@linkplain #close() closed}</li>
     * </ul>
     * <p>If the current thread:</p>
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * <p>then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.</p>
     *
     * @throws InterruptedException if the current thread is interrupted
     *                              while waiting
     * @throws EOFException         if the instance is {@linkplain #close() closed}
     *                              while waiting
     */
    public void await() throws InterruptedException, EOFException
    {
        synchronized(this)
        {
            // 一直等待，直到锁可用
            while ( (state.get() == State.STARTED) && !hasLeadership.get() )
            {
                wait();
            }
        }
        if ( state.get() != State.STARTED )
        {
            // 这个异常真的是.....
            throw new EOFException();
        }
    }

    /**
     * 使当前线程阻塞等待当前实例成为leader，直到申请成功或 被中断 或 指定的时间过去(超时) 或 当前实例被关闭。
     * 如果当前线程已经是leader，那么该方法将立即返回。
     *
     * <p>Causes the current thread to wait until this instance acquires leadership
     * unless the thread is {@linkplain Thread#interrupt interrupted},
     * the specified waiting time elapses or the instance is {@linkplain #close() closed}.</p>
     * <p></p>
     * <p>If this instance already is the leader then this method returns immediately
     * with the value {@code true}.</p>
     * <p></p>
     * <p>Otherwise the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of four things happen:</p>
     * <ul>
     * <li>This instance becomes the leader</li>
     * <li>Some other thread {@linkplain Thread#interrupt interrupts}
     * the current thread</li>
     * <li>The specified waiting time elapses.</li>
     * <li>The instance is {@linkplain #close() closed}</li>
     * </ul>
     * <p></p>
     * <p>If the current thread:</p>
     * <ul>
     * <li>has its interrupted status set on entry to this method; or
     * <li>is {@linkplain Thread#interrupt interrupted} while waiting,
     * </ul>
     * <p>then {@link InterruptedException} is thrown and the current thread's
     * interrupted status is cleared.</p>
     * <p></p>
     * <p>If the specified waiting time elapses or the instance is {@linkplain #close() closed}
     * then the value {@code false} is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.</p>
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the {@code timeout} argument
     * @return {@code true} if the count reached zero and {@code false}
     * if the waiting time elapsed before the count reached zero or the instances was closed
     * @throws InterruptedException if the current thread is interrupted
     *                              while waiting
     */
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException
    {
        long waitNanos = TimeUnit.NANOSECONDS.convert(timeout, unit);

        synchronized(this)
        {
            // 时间足够且还未成为leader的情况下继续等待
            while ( true )
            {
                if ( state.get() != State.STARTED )
                {
                    return false;
                }

                if ( hasLeadership() )
                {
                    return true;
                }

                if ( waitNanos <= 0 )
                {
                    return false;
                }

                long startNanos = System.nanoTime();
                TimeUnit.NANOSECONDS.timedWait(this, waitNanos);
                long elapsed = System.nanoTime() - startNanos;
                waitNanos -= elapsed;
            }
        }
    }

    /**
     * Return this instance's participant Id
     *
     * @return participant Id
     */
    public String getId()
    {
        return id;
    }

    /**
     * Returns this instances current state, this is the only way to verify that the object has been closed before
     * closing again.  If you try to close a latch multiple times, the close() method will throw an
     * IllegalArgumentException which is often not caught and ignored (CloseableUtils.closeQuietly() only looks for
     * IOException).
     *
     * @return the state of the current instance
     */
    public State getState()
    {
        return state.get();
    }

    /**
     * 获取参与选举的用户信息。
     * 注意：该方法字节从zookeeper中拉取数据。因此其返回值可能和{@link #hasLeadership()}并不一致，
     * 因为{@link #hasLeadership()}使用的是本地变量。
     *
     * <p>
     * Returns the set of current participants in the leader selection
     * </p>
     * <p>
     * <p>
     * <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     * return a value that does not match {@link #hasLeadership()} as hasLeadership
     * uses a local field of the class.
     * </p>
     *
     * @return participants
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Collection<Participant> getParticipants() throws Exception
    {
        Collection<String> participantNodes = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter);
        return LeaderSelector.getParticipants(client, participantNodes);
    }

    /**
     * 获取当前的leader信息。
     * 如果由于某种原因，这里不存在一个leader，将返回一个假的参与者信息。
     * <b>注意</b> - 该方法直接查询zookeeper服务器，因此其返回值可能和{@link #hasLeadership()}并不匹配，
     * 因为{@link #hasLeadership()}使用的是本地变量。
     *
     * <p>
     * Return the id for the current leader. If for some reason there is no
     * current leader, a dummy participant is returned.
     * </p>
     * <p>
     * <p>
     * <B>NOTE</B> - this method polls the ZK server. Therefore it can possibly
     * return a value that does not match {@link #hasLeadership()} as hasLeadership
     * uses a local field of the class.
     * </p>
     *
     * @return leader
     * @throws Exception ZK errors, interruptions, etc.
     */
    public Participant getLeader() throws Exception
    {
        Collection<String> participantNodes = LockInternals.getParticipantNodes(client, latchPath, LOCK_NAME, sorter);
        return LeaderSelector.getLeader(client, participantNodes);
    }

    /**
     * 查询当前是否是leader。
     * Return true if leadership is currently held by this instance
     *
     * @return true/false
     */
    public boolean hasLeadership()
    {
        // 查询的是缓存值
        return (state.get() == State.STARTED) && hasLeadership.get();
    }

    @VisibleForTesting
    String getOurPath()
    {
        return ourPath.get();
    }

    @VisibleForTesting
    volatile CountDownLatch debugResetWaitLatch = null;

    /**
     * 重置选举状态 - 只有当确定不是leader的时候才应该调用
     */
    @VisibleForTesting
    void reset() throws Exception
    {
        // 取消leader，删除本地信息，重新创建节点参与leader选举
        setLeadership(false);
        setNode(null);

        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( debugResetWaitLatch != null )
                {
                    // 等待测试组件执行
                    debugResetWaitLatch.await();
                    debugResetWaitLatch = null;
                }

                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    setNode(event.getName());
                    if ( state.get() == State.CLOSED )
                    {
                        setNode(null);
                    }
                    else
                    {
                        getChildren();
                    }
                }
                else
                {
                    log.error("getChildren() failed. rc = " + event.getResultCode());
                }
            }
        };
        // 重新创建节点
        client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).inBackground(callback).forPath(ZKPaths.makePath(latchPath, LOCK_NAME), LeaderSelector.getIdBytes(id));
    }

    /**
     * 真正的启动逻辑 - 该方法和{@link #close()}有竞争关系，因此都是加锁的。
     */
    private synchronized void internalStart()
    {
        if ( state.get() == State.STARTED )
        {
            // 添加连接监听器
            client.getConnectionStateListenable().addListener(listener);
            try
            {
                // 初始化节点状态
                reset();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                log.error("An error occurred checking resetting leadership.", e);
            }
        }
    }

    @VisibleForTesting
    volatile CountDownLatch debugCheckLeaderShipLatch = null;

    private void checkLeadership(List<String> children) throws Exception
    {
        if ( debugCheckLeaderShipLatch != null )
        {
            debugCheckLeaderShipLatch.await();
        }

        final String localOurPath = ourPath.get();
        List<String> sortedChildren = LockInternals.getSortedChildren(LOCK_NAME, sorter, children);
        int ourIndex = (localOurPath != null) ? sortedChildren.indexOf(ZKPaths.getNodeFromPath(localOurPath)) : -1;
        if ( ourIndex < 0 )
        {
            // 找不到自己创建的节点 - 调用了reset/close 或 出现意料之外的错误
            log.error("Can't find our node. Resetting. Index: " + ourIndex);
            reset();
        }
        else if ( ourIndex == 0 )
        {
            // 我的节点排在第一位，我就是leader
            setLeadership(true);
        }
        else
        {
            // 我不是leader，那么我只需要检测我前面一位的删除事件
            String watchPath = sortedChildren.get(ourIndex - 1);
            Watcher watcher = new Watcher()
            {
                @Override
                public void process(WatchedEvent event)
                {
                    if ( (state.get() == State.STARTED) && (event.getType() == Event.EventType.NodeDeleted) && (localOurPath != null) )
                    {
                        try
                        {
                            // 检测到前一位删除事件的时候，再次检查我是不是leader
                            getChildren();
                        }
                        catch ( Exception ex )
                        {
                            ThreadUtils.checkInterrupted(ex);
                            log.error("An error occurred checking the leadership.", ex);
                        }
                    }
                }
            };
            // 拉取数据的回调逻辑
            BackgroundCallback callback = new BackgroundCallback()
            {
                @Override
                public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
                {
                    if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                    {
                        // previous node is gone - reset
                        reset();
                    }
                }
            };
            // 使用 getData() 而不是 exist() 是为了避免留下不必要的观察者，这是一种资源/内存泄漏。
            // 我的理解是这样的：
            // exist 会在节点 创建、删除、改变的时候产生事件， 而getData会在节点 删除、改变的时候产生事件，节点不存在的时候watcher不生效。
            // use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
            client.getData().usingWatcher(watcher).inBackground(callback).forPath(ZKPaths.makePath(latchPath, watchPath));
        }
    }

    /**
     * 后台拉取所有参与者信息
     */
    private void getChildren() throws Exception
    {
        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    // 拉取成功，检查是否是leader - 我系不系排第一
                    checkLeadership(event.getChildren());
                }
            }
        };
        client.getChildren().inBackground(callback).forPath(ZKPaths.makePath(latchPath, null));
    }

    /**
     * 处理连接状态改变事件
     * @param newState 最新连接状态
     */
    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
            default:
            {
                // NOP
                break;
            }

            case RECONNECTED:
            {
                try
                {
                    if ( client.getConnectionStateErrorPolicy().isErrorState(ConnectionState.SUSPENDED) || !hasLeadership.get() )
                    {
                        reset();
                    }
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    log.error("Could not reset leader latch", e);
                    setLeadership(false);
                }
                break;
            }

            case SUSPENDED:
            {
                if ( client.getConnectionStateErrorPolicy().isErrorState(ConnectionState.SUSPENDED) )
                {
                    setLeadership(false);
                }
                break;
            }

            case LOST:
            {
                setLeadership(false);
                break;
            }
        }
    }

    /**
     * 设置leader状态
     */
    private synchronized void setLeadership(boolean newValue)
    {
        boolean oldValue = hasLeadership.getAndSet(newValue);

        if ( oldValue && !newValue )
        { // Lost leadership, was true, now false
            // true -> false，表示失去leader，成为了follower
            listeners.forEach(new Function<LeaderLatchListener, Void>()
                {
                    @Override
                    public Void apply(LeaderLatchListener listener)
                    {
                        listener.notLeader();
                        return null;
                    }
                });
        }
        else if ( !oldValue && newValue )
        { // Gained leadership, was false, now true
            // false -> true，表示有follower成为leader
            listeners.forEach(new Function<LeaderLatchListener, Void>()
                {
                    @Override
                    public Void apply(LeaderLatchListener input)
                    {
                        input.isLeader();
                        return null;
                    }
                });
        }
        // 通知在该对象上等待成为leader的线程。
        notifyAll();
    }

    private void setNode(String newValue) throws Exception
    {
        String oldPath = ourPath.getAndSet(newValue);
        if ( oldPath != null )
        {
            // guaranteed - 确保一定删除，否则一旦存在该会话的无效节点，将导致严重错误。
            client.delete().guaranteed().inBackground().forPath(oldPath);
        }
    }
}
