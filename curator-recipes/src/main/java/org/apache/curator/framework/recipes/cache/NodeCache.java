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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * zookeeper单节点缓存。
 * 一个尝试保留指定Node的数据的本地缓存工具类。该类会观察指定节点，响应节点增删该事件，拉取节点数据等等。
 * 你可以在该类对象上注册监听器，当节点状态/数据发生改变时，监听器将会收到通知。
 *
 * <b>非常重要</b>
 * 该类不能保持事务同步！该类的使用者必须为 假正(FP) 和假负(FN)作出准备。此外，在更新数据时始终使用版本号，避免覆盖其它进程做出的更改。
 * --
 * 和{@link PathChildrenCache}文档很像，也有很多类似的问题。
 *
 * 注意：
 * 1. 在缓存启动前注册监听器。
 *
 * <h3>事件处理</h3>
 * {@link #setNewData(ChildData)}操作由zk线程 main-EventThread 执行，即数据更新操作由 main-EventThread 执行。
 * 1. 如果你在注册监听器时没有指定executor，那么事件处理也由 main-EventThread线程执行，在处理事件时可以直接从NodeCache中获取数据，
 * {@link #getCurrentData()}获取到的数据是与事件匹配的。
 *
 * 2. 如果你指定了处理使用的executor！你在处理事件，由于事件中不包含数据，只能通过{@link #getCurrentData()}获取数据，将无法获得与事件真正匹配的数据。
 * 但是最终能得到一致的结果。因为数据是最终一致的！收到的事件数是相同的，那么最终结果是一样的，但个人觉得不算个好设计。
 *
 *
 * <p>A utility that attempts to keep the data from a node locally cached. This class
 * will watch the node, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 *
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 */
public class NodeCache implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WatcherRemoveCuratorFramework client;
    private final String path;
    /** 数据是否采用了压缩存储 */
    private final boolean dataIsCompressed;
    /** 节点的当前数据 */
    private final AtomicReference<ChildData> data = new AtomicReference<ChildData>(null);
    /** 缓存的生命周期标识 */
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    /** 监听器管理器(监听器容器) */
    private final ListenerContainer<NodeCacheListener> listeners = new ListenerContainer<NodeCacheListener>();
    /** zk客户端是否建立着连接 */
    private final AtomicBoolean isConnected = new AtomicBoolean(true);
    /** 连接状态监听器 */
    private ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            // 建立了连接或是重连
            if ( (newState == ConnectionState.CONNECTED) || (newState == ConnectionState.RECONNECTED) )
            {
                if ( isConnected.compareAndSet(false, true) )
                {
                    // 如果之前检测到了断开连接，首次连接或重连之后需要重置缓存
                    try
                    {
                        reset();
                    }
                    catch ( Exception e )
                    {
                        // 检查是否需要恢复中断
                        ThreadUtils.checkInterrupted(e);
                        log.error("Trying to reset after reconnection", e);
                    }
                }
            }
            else
            {
                // 断开了连接
                isConnected.set(false);
            }
        }
    };

    /** 节点watcher */
    private Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            // 当节点状态发生改变，重新拉取节点数据
            try
            {
                reset();
            }
            catch(Exception e)
            {
                // 检查是否需要恢复中断 和 处理异常。
                // zookeeper中很多这中检测，是因为抛出的都是受检异常Exception...
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        }
    };

    // 缓存状态
    private enum State
    {
        /** 初始状态 */
        LATENT,
        /** 已启动状态，运行状态 */
        STARTED,
        /** 已关闭状态 */
        CLOSED
    }

    /** 拉取节点数据完成的回调 */
    private final BackgroundCallback backgroundCallback = new BackgroundCallback()
    {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            // 处理事件，主要有get和checkExists事件
            processBackgroundResult(event);
        }
    };

    /**
     * @param client curztor client
     * @param path the full path to the node to cache 要缓存的节点路径
     */
    public NodeCache(CuratorFramework client, String path)
    {
        this(client, path, false);
    }

    /**
     * @param client curztor client
     * @param path the full path to the node to cache 要缓存的节点路径
     * @param dataIsCompressed if true, data in the path is compressed
     *                         如果为true，表示zk中存储的数据是压缩格式
     */
    public NodeCache(CuratorFramework client, String path, boolean dataIsCompressed)
    {
        this.client = client.newWatcherRemoveCuratorFramework();
        this.path = PathUtils.validatePath(path);
        this.dataIsCompressed = dataIsCompressed;
    }

    public CuratorFramework getClient()
    {
        return client;
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void     start() throws Exception
    {
        start(false);
    }

    /**
     * 启动缓存。缓存并不会自动启动，你必须调用start方法启动缓存。
     * 你可以选择启动时是否构建缓存的初始视图。
     *
     * 如果要构建缓存的初始化视图，在返回前会拉取一次节点的当前数据，再开启监听（多拉取一次数据），拉取初始数据不会抛出事件。
     * 不过对于NodeCache而言，只维护一个节点数据，多拉取一次数据一般没有太大消耗，但是对于{@link PathChildrenCache}就得仔细考虑。
     *
     * Same as {@link #start()} but gives the option of doing an initial build
     *
     * @param buildInitial if true, {@link #rebuild()} will be called before this method
     *                     returns in order to get an initial view of the node
     *                     是否构建初始缓存。如果为true，表示在方法返回之前会调用{@link #rebuild()}方法，
     *                     以构建缓存的初始化视图。
     *
     * @throws Exception errors
     */
    public void     start(boolean buildInitial) throws Exception
    {
        // 只允许启动一次
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        // 监听连接状态
        client.getConnectionStateListenable().addListener(connectionStateListener);

        if ( buildInitial )
        {
            // 注意：创建了必要的父节点，并没有创建自己
            client.checkExists().creatingParentContainersIfNeeded().forPath(path);
            // 获取节点数据
            internalRebuild();
        }
        //
        reset();
    }

    /**
     * 关闭缓存，释放资源
     * @throws IOException error
     */
    @Override
    public void close() throws IOException
    {
        // 只能从运行状态，切换到关闭状态
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.removeWatchers();
            listeners.clear();
            client.getConnectionStateListenable().removeListener(connectionStateListener);

            // 显式置为null，可以帮助GC更快的回收这些不再使用的对象
            // TODO
            // From PathChildrenCache
            // This seems to enable even more GC - I'm not sure why yet - it
            // has something to do with Guava's cache and circular references
            connectionStateListener = null;
            watcher = null;
        }        
    }

    /**
     * 获取缓存的监听管理器，可以通过{@link ListenerContainer#addListener(Object)}等方法添加和移除监听器。
     * Return the cache listenable
     *
     * @return listenable
     */
    public ListenerContainer<NodeCacheListener> getListenable()
    {
        Preconditions.checkState(state.get() != State.CLOSED, "Closed");

        return listeners;
    }

    /**
     * 重建缓存。
     * 注意：这是一个阻塞方法。通过查询所有需要的数据重新完整构建内部的缓存，并且不生成任何的时间给监听器们。
     *
     * NOTE: this is a BLOCKING method. Completely rebuild the internal cache by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @throws Exception errors
     */
    public void     rebuild() throws Exception
    {
        Preconditions.checkState(state.get() == State.STARTED, "Not started");
        // 拉取节点最新数据
        internalRebuild();
        // 重新拉取数据
        reset();
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If the node does not exist,
     * this returns null
     *
     * @return data or null
     */
    public ChildData getCurrentData()
    {
        return data.get();
    }

    /**
     * Return the path this cache is watching
     *
     * @return path
     */
    public String getPath()
    {
        return path;
    }

    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;

    /**
     * 虽然叫reset(重置)，看做refresh可能更容易理解一些。
     * @throws Exception zk errors
     */
    private void     reset() throws Exception
    {
        if ( (state.get() == State.STARTED) && isConnected.get() )
        {
            client.checkExists().creatingParentContainersIfNeeded().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
        }
    }

    /**
     * 尝试重新构建节点数据
     */
    private void     internalRebuild() throws Exception
    {
        try
        {
            Stat    stat = new Stat();
            byte[]  bytes = dataIsCompressed ? client.getData().decompressed().storingStatIn(stat).forPath(path) : client.getData().storingStatIn(stat).forPath(path);
            // 拉取节点数据成功，更新缓存
            data.set(new ChildData(path, stat, bytes));
        }
        catch ( KeeperException.NoNodeException e )
        {
            // 发现节点不存在，将本地节点置为null
            data.set(null);
        }
    }

    /**
     * 处理后台拉取到的数据
     * @param event 后台操作对应的结果
     * @throws Exception errors
     */
    private void processBackgroundResult(CuratorEvent event) throws Exception
    {
        // 该节点只会发起getData和checkExists操作
        switch ( event.getType() )
        {
            case GET_DATA:
            {
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    // 如果获取数据成功，则应用新数据到本地缓存
                    ChildData childData = new ChildData(path, event.getStat(), event.getData());
                    setNewData(childData);
                }
                break;
            }

            case EXISTS:
            {
                if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    // 如果检查到节点不存在，则将本地数据置为null
                    setNewData(null);
                }
                else if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    // 检测到节点存在，发起拉取数据请求
                    if ( dataIsCompressed )
                    {
                        client.getData().decompressed().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
                    }
                    else
                    {
                        client.getData().usingWatcher(watcher).inBackground(backgroundCallback).forPath(path);
                    }
                }
                break;
            }
        }
    }

    private void setNewData(ChildData newData) throws InterruptedException
    {
        ChildData   previousData = data.getAndSet(newData);
        if ( !Objects.equal(previousData, newData) )
        {
            // 提交事件到目标线程，这里其实存在一些问题，nodeChanged真正执行的时候，这里的数据可能又产生了变化，
            // 如果nodeChanged执行在别的线程，做出来的推断可能是错误的！比如从NodeCache中取出来最新数据为null，你推断此时数据被删除了，但其实你当前的事件可能只是节点数据改变产生的。
            listeners.forEach
            (
                new Function<NodeCacheListener, Void>()
                {
                    @Override
                    public Void apply(NodeCacheListener listener)
                    {
                        try
                        {
                            listener.nodeChanged();
                        }
                        catch ( Exception e )
                        {
                            // 检查中断是个好习惯，避免错误的捕获异常
                            ThreadUtils.checkInterrupted(e);
                            log.error("Calling listener", e);
                        }
                        return null;
                    }
                }
            );

            // 唤醒测试组件
            if ( rebuildTestExchanger != null )
            {
                try
                {
                    rebuildTestExchanger.exchange(new Object());
                }
                catch ( InterruptedException e )
                {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }
    
    /**
     * 处理异常，默认仅仅记录到日志。
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void handleException(Throwable e)
    {
        log.error("", e);
    }
}
