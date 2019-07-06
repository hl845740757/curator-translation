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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.EnsureContainers;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.CloseableExecutorService;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 一个尝试将指定zk路径下的所有子节点数据保存下来的实用本地缓存。
 * 该类将会watch指定的zk路径，响应子节点的增删改事件，并拉取所有的数据等。
 * 你可以注册一个{@link PathChildrenCacheListener}，当产生改变的时候，listener将会得到通知。
 *
 * <b>非常重要</b>
 * 该类不能保持事务同步！该类的使用者必须为 假正(FP) 和假负(FN)作出准备。
 * 此外，在更新数据时始终使用版本号，避免覆盖其它进程做出的更改。
 *
 * 注意我在{@link PathChildrenCacheEvent}中提到的安全性问题，不要在处理事件的时候使用{@link PathChildrenCache}中的数据。
 *
 * (假正，假负介绍)
 * - https://blog.csdn.net/xbinworld/article/details/50631342
 *
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 * <p></p>
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 */
@SuppressWarnings("NullableProblems")
public class PathChildrenCache implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final CuratorFramework client;
    /** 缓存节点路径 */
    private final String path;
    /**
     * 该缓存所有操作的执行线程。负责拉取{@link #path}下的数据，并处理事件。
     */
    private final CloseableExecutorService executorService;
    /**
     * 是否缓存节点数据，默认缓存。
     * 一般来说我们使用Cache就是为了缓存数据的。
     */
    private final boolean cacheData;
    /** 数据是否是进行了压缩 */
    private final boolean dataIsCompressed;
    /**
     * 该缓存上的所有监听器，需要在调用{@link #start()}之前注册监听器。
     * why?
     * 1. 如果你基于{@link #currentData}决定是否进行监听，当你注册监听器的时候，你的检查结果可能是无效的！（无法控制main-thread更新data）
     * 2. 在启动后进行监听，可能无法获得完整的数据和事件。
     */
    private final ListenerContainer<PathChildrenCacheListener> listeners = new ListenerContainer<PathChildrenCacheListener>();
    /**
     * 节点下数据的本地缓存。
     * 重要的事情说三遍：该数据由 main-EventThread进行更新！该数据由 main-EventThread进行更新！该数据由 main-EventThread进行更新！
     * 其它线程如果仅仅是偶尔需要以下当前的最新数据，那么可以使用{@link #getCurrentData()}。
     * 如果要处理事件，那么最好不要使用{@link #getCurrentData()}。
     */
    private final ConcurrentMap<String, ChildData> currentData = Maps.newConcurrentMap();
    private final AtomicReference<Map<String, ChildData>> initialSet = new AtomicReference<Map<String, ChildData>>();
    private final Set<Operation> operationsQuantizer = Sets.newSetFromMap(Maps.<Operation, Boolean>newConcurrentMap());
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    private final EnsureContainers ensureContainers;

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private static final ChildData NULL_CHILD_DATA = new ChildData("/", null, null);

    private static final boolean USE_EXISTS = Boolean.getBoolean("curator-path-children-cache-use-exists");

    private volatile Watcher childrenWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            offerOperation(new RefreshOperation(PathChildrenCache.this, RefreshMode.STANDARD));
        }
    };

    private volatile Watcher dataWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try
            {
                if ( event.getType() == Event.EventType.NodeDeleted )
                {
                    remove(event.getPath());
                }
                else if ( event.getType() == Event.EventType.NodeDataChanged )
                {
                    offerOperation(new GetDataOperation(PathChildrenCache.this, event.getPath()));
                }
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        }
    };

    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;

    private volatile ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };
    private static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("PathChildrenCache");

    /**
     * @param client the client
     * @param path   path to watch
     * @param mode   caching mode
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(defaultThreadFactory), true));
    }

    /**
     * @param client        the client
     * @param path          path to watch
     * @param mode          caching mode
     * @param threadFactory factory to use when creating internal threads
     * @deprecated use {@link #PathChildrenCache(CuratorFramework, String, boolean, ThreadFactory)} instead
     */
    @Deprecated
    @SuppressWarnings("deprecation")
    public PathChildrenCache(CuratorFramework client, String path, PathChildrenCacheMode mode, ThreadFactory threadFactory)
    {
        this(client, path, mode != PathChildrenCacheMode.CACHE_PATHS_ONLY, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client    the client
     * @param path      path to watch
     * @param cacheData if true, node contents are cached in addition to the stat
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)
    {
        this(client, path, cacheData, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(defaultThreadFactory), true));
    }

    /**
     * @param client        the client
     * @param path          path to watch
     * @param cacheData     if true, node contents are cached in addition to the stat
     * @param threadFactory factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, ThreadFactory threadFactory)
    {
        this(client, path, cacheData, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param threadFactory    factory to use when creating internal threads
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, ThreadFactory threadFactory)
    {
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  ExecutorService to use for the PathChildrenCache's background thread. This service should be single threaded, otherwise the cache may see inconsistent results.
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final ExecutorService executorService)
    {
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(executorService));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  Closeable ExecutorService to use for the PathChildrenCache's background thread. This service should be single threaded, otherwise the cache may see inconsistent results.
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final CloseableExecutorService executorService)
    {
        this.client = client;
        this.path = PathUtils.validatePath(path);
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        this.executorService = executorService;
        ensureContainers = new EnsureContainers(client, path);
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        start(StartMode.NORMAL);
    }

    /**
     * Same as {@link #start()} but gives the option of doing an initial build
     *
     * @param buildInitial if true, {@link #rebuild()} will be called before this method
     *                     returns in order to get an initial view of the node; otherwise,
     *                     the cache will be initialized asynchronously
     * @throws Exception errors
     * @deprecated use {@link #start(StartMode)}
     */
    @Deprecated
    public void start(boolean buildInitial) throws Exception
    {
        start(buildInitial ? StartMode.BUILD_INITIAL_CACHE : StartMode.NORMAL);
    }

    /**
     * Method of priming cache on {@link PathChildrenCache#start(StartMode)}
     */
    public enum StartMode
    {
        /**
         * The cache will be primed (in the background) with initial values.
         * Events for existing and new nodes will be posted.
         */
        NORMAL,

        /**
         * The cache will be primed (in the foreground) with initial values.
         * {@link PathChildrenCache#rebuild()} will be called before
         * the {@link PathChildrenCache#start(StartMode)} method returns
         * in order to get an initial view of the node.
         */
        BUILD_INITIAL_CACHE,

        /**
         * After cache is primed with initial values (in the background) a
         * {@link PathChildrenCacheEvent.Type#INITIALIZED} will be posted.
         */
        POST_INITIALIZED_EVENT
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @param mode Method for priming the cache
     * @throws Exception errors
     */
    public void start(StartMode mode) throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");
        mode = Preconditions.checkNotNull(mode, "mode cannot be null");

        client.getConnectionStateListenable().addListener(connectionStateListener);

        switch ( mode )
        {
            case NORMAL:
            {
                offerOperation(new RefreshOperation(this, RefreshMode.STANDARD));
                break;
            }

            case BUILD_INITIAL_CACHE:
            {
                rebuild();
                break;
            }

            case POST_INITIALIZED_EVENT:
            {
                initialSet.set(Maps.<String, ChildData>newConcurrentMap());
                offerOperation(new RefreshOperation(this, RefreshMode.POST_INITIALIZED));
                break;
            }
        }
    }

    /**
     * NOTE: this is a BLOCKING method. Completely rebuild the internal cache by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @throws Exception errors
     */
    public void rebuild() throws Exception
    {
        Preconditions.checkState(!executorService.isShutdown(), "cache has been closed");

        ensurePath();

        clear();

        List<String> children = client.getChildren().forPath(path);
        for ( String child : children )
        {
            String fullPath = ZKPaths.makePath(path, child);
            internalRebuildNode(fullPath);

            if ( rebuildTestExchanger != null )
            {
                rebuildTestExchanger.exchange(new Object());
            }
        }

        // this is necessary so that any updates that occurred while rebuilding are taken
        offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
    }

    /**
     * NOTE: this is a BLOCKING method. Rebuild the internal cache for the given node by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @param fullPath full path of the node to rebuild
     * @throws Exception errors
     */
    public void rebuildNode(String fullPath) throws Exception
    {
        Preconditions.checkArgument(ZKPaths.getPathAndNode(fullPath).getPath().equals(path), "Node is not part of this cache: " + fullPath);
        Preconditions.checkState(!executorService.isShutdown(), "cache has been closed");

        ensurePath();
        internalRebuildNode(fullPath);

        // this is necessary so that any updates that occurred while rebuilding are taken
        // have to rebuild entire tree in case this node got deleted in the interim
        offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
    }

    /**
     * Close/end the cache
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            listeners.clear();
            executorService.close();
            client.clearWatcherReferences(childrenWatcher);
            client.clearWatcherReferences(dataWatcher);

            // TODO
            // This seems to enable even more GC - I'm not sure why yet - it
            // has something to do with Guava's cache and circular references
            connectionStateListener = null;
            childrenWatcher = null;
            dataWatcher = null;
        }
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public ListenerContainer<PathChildrenCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    public List<ChildData> getCurrentData()
    {
        return ImmutableList.copyOf(Sets.<ChildData>newTreeSet(currentData.values()));
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no child with that path, <code>null</code>
     * is returned.
     *
     * @param fullPath full path to the node to check
     * @return data or null
     */
    public ChildData getCurrentData(String fullPath)
    {
        return currentData.get(fullPath);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath the path of the node to clear
     */
    public void clearDataBytes(String fullPath)
    {
        clearDataBytes(fullPath, -1);
    }

    /**
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath  the path of the node to clear
     * @param ifVersion if non-negative, only clear the data if the data's version matches this version
     * @return true if the data was cleared
     */
    public boolean clearDataBytes(String fullPath, int ifVersion)
    {
        ChildData data = currentData.get(fullPath);
        if ( data != null )
        {
            if ( (ifVersion < 0) || (ifVersion == data.getStat().getVersion()) )
            {
                if ( data.getData() != null )
                {
                    currentData.replace(fullPath, data, new ChildData(data.getPath(), data.getStat(), null));
                }
                return true;
            }
        }
        return false;
    }

    /**
     * Clear out current data and begin a new query on the path
     *
     * @throws Exception errors
     */
    public void clearAndRefresh() throws Exception
    {
        currentData.clear();
        offerOperation(new RefreshOperation(this, RefreshMode.STANDARD));
    }

    /**
     * Clears the current data without beginning a new query and without generating any events
     * for listeners.
     */
    public void clear()
    {
        currentData.clear();
    }

    enum RefreshMode
    {
        STANDARD,
        FORCE_GET_DATA_AND_STAT,
        POST_INITIALIZED
    }

    void refresh(final RefreshMode mode) throws Exception
    {
        ensurePath();

        final BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if (PathChildrenCache.this.state.get().equals(State.CLOSED)) {
                    // This ship is closed, don't handle the callback
                    return;
                }
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    processChildren(event.getChildren(), mode);
                }
            }
        };

        client.getChildren().usingWatcher(childrenWatcher).inBackground(callback).forPath(path);
    }

    void callListeners(final PathChildrenCacheEvent event)
    {
        listeners.forEach
            (
                new Function<PathChildrenCacheListener, Void>()
                {
                    @Override
                    public Void apply(PathChildrenCacheListener listener)
                    {
                        try
                        {
                            listener.childEvent(client, event);
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            handleException(e);
                        }
                        return null;
                    }
                }
            );
    }

    void getDataAndStat(final String fullPath) throws Exception
    {
        BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                applyNewData(fullPath, event.getResultCode(), event.getStat(), cacheData ? event.getData() : null);
            }
        };

        if ( USE_EXISTS && !cacheData )
        {
            // 如果使用exist 并且不缓存数据，则走到这里，一般不会走到这里
            client.checkExists().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
        }
        else
        {


            // always use getData() instead of exists() to avoid leaving unneeded watchers which is a type of resource leak
            if ( dataIsCompressed && cacheData )
            {
                client.getData().decompressed().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
            }
            else
            {
                client.getData().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
            }
        }
    }

    /**
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void handleException(Throwable e)
    {
        log.error("", e);
    }

    protected void ensurePath() throws Exception
    {
        ensureContainers.ensure();
    }

    @VisibleForTesting
    protected void remove(String fullPath)
    {
        ChildData data = currentData.remove(fullPath);
        if ( data != null )
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, data)));
        }

        Map<String, ChildData> localInitialSet = initialSet.get();
        if ( localInitialSet != null )
        {
            localInitialSet.remove(fullPath);
            maybeOfferInitializedEvent(localInitialSet);
        }
    }

    private void internalRebuildNode(String fullPath) throws Exception
    {
        if ( cacheData )
        {
            try
            {
                Stat stat = new Stat();
                byte[] bytes = dataIsCompressed ? client.getData().decompressed().storingStatIn(stat).forPath(fullPath) : client.getData().storingStatIn(stat).forPath(fullPath);
                currentData.put(fullPath, new ChildData(fullPath, stat, bytes));
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // node no longer exists - remove it
                currentData.remove(fullPath);
            }
        }
        else
        {
            Stat stat = client.checkExists().forPath(fullPath);
            if ( stat != null )
            {
                currentData.put(fullPath, new ChildData(fullPath, stat, null));
            }
            else
            {
                // node no longer exists - remove it
                currentData.remove(fullPath);
            }
        }
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
        case SUSPENDED:
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED, null)));
            break;
        }

        case LOST:
        {
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_LOST, null)));
            break;
        }

        case CONNECTED:
        case RECONNECTED:
        {
            try
            {
                offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED, null)));
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
            break;
        }
        }
    }

    private void processChildren(List<String> children, RefreshMode mode) throws Exception
    {
        Set<String> removedNodes = Sets.newHashSet(currentData.keySet());
        for ( String child : children ) {
            removedNodes.remove(ZKPaths.makePath(path, child));
        }

        for ( String fullPath : removedNodes )
        {
            remove(fullPath);
        }

        for ( String name : children )
        {
            String fullPath = ZKPaths.makePath(path, name);

            if ( (mode == RefreshMode.FORCE_GET_DATA_AND_STAT) || !currentData.containsKey(fullPath) )
            {
                getDataAndStat(fullPath);
            }

            updateInitialSet(name, NULL_CHILD_DATA);
        }
        maybeOfferInitializedEvent(initialSet.get());
    }

    /**
     * 应用新数据，将拉取到的新数据保存到缓存中。
     * 注意：由于拉取数据是后台执行，由main-EventThread执行回调。
     * 数据的更新是 main-EventThread立即执行的，而事件处理被提交给了{@link #executorService}。
     * @param fullPath 子节点的全路径
     * @param resultCode 后台执行时的结果码，表示操作是否成功
     * @param stat 节点的最新状态
     * @param bytes 节点的最新数据
     */
    private void applyNewData(String fullPath, int resultCode, Stat stat, byte[] bytes)
    {
        if ( resultCode == KeeperException.Code.OK.intValue() ) // otherwise - node must have dropped or something - we should be getting another event
        {
            ChildData data = new ChildData(fullPath, stat, bytes);
            ChildData previousData = currentData.put(fullPath, data);
            if ( previousData == null ) // i.e. new
            {
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, data)));
            }
            else if ( previousData.getStat().getVersion() != stat.getVersion() )
            {
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data)));
            }
            updateInitialSet(ZKPaths.getNodeFromPath(fullPath), data);
        }
    }

    private void updateInitialSet(String name, ChildData data)
    {
        Map<String, ChildData> localInitialSet = initialSet.get();
        if ( localInitialSet != null )
        {
            localInitialSet.put(name, data);
            maybeOfferInitializedEvent(localInitialSet);
        }
    }

    private void maybeOfferInitializedEvent(Map<String, ChildData> localInitialSet)
    {
        if ( !hasUninitialized(localInitialSet) )
        {
            // all initial children have been processed - send initialized message

            if ( initialSet.getAndSet(null) != null )   // avoid edge case - don't send more than 1 INITIALIZED event
            {
                final List<ChildData> children = ImmutableList.copyOf(localInitialSet.values());
                PathChildrenCacheEvent event = new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.INITIALIZED, null)
                {
                    @Override
                    public List<ChildData> getInitialData()
                    {
                        return children;
                    }
                };
                offerOperation(new EventOperation(this, event));
            }
        }
    }

    private boolean hasUninitialized(Map<String, ChildData> localInitialSet)
    {
        if ( localInitialSet == null )
        {
            return false;
        }

        Map<String, ChildData> uninitializedChildren = Maps.filterValues
            (
                localInitialSet,
                new Predicate<ChildData>()
                {
                    @Override
                    public boolean apply(ChildData input)
                    {
                        return (input == NULL_CHILD_DATA);  // check against ref intentional
                    }
                }
            );
        return (uninitializedChildren.size() != 0);
    }

    void offerOperation(final Operation operation)
    {
        if ( operationsQuantizer.add(operation) )
        {
            submitToExecutor
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            operationsQuantizer.remove(operation);
                            operation.invoke();
                        }
                        catch ( InterruptedException e )
                        {
                            //We expect to get interrupted during shutdown,
                            //so just ignore these events
                            if ( state.get() != State.CLOSED )
                            {
                                handleException(e);
                            }
                            Thread.currentThread().interrupt();
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            handleException(e);
                        }
                    }
                }
            );
        }
    }

    /**
     * Submits a runnable to the executor.
     * <p>
     * This method is synchronized because it has to check state about whether this instance is still open.  Without this check
     * there is a race condition with the dataWatchers that get set.  Even after this object is closed() it can still be
     * called by those watchers, because the close() method cannot actually disable the watcher.
     * <p>
     * The synchronization overhead should be minimal if non-existant as this is generally only called from the
     * ZK client thread and will only contend if close() is called in parallel with an update, and that's the exact state
     * we want to protect from.
     *
     * @param command The runnable to run
     */
    private synchronized void submitToExecutor(final Runnable command)
    {
        if ( state.get() == State.STARTED )
        {
            executorService.submit(command);
        }
    }
}
