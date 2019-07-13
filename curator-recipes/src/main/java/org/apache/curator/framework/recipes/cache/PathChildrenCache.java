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
 * 该类不能保持事务同步！该类的使用者必须为 假正(FP) 和假负(FN)作出准备。此外，在更新数据时始终使用版本号，避免覆盖其它进程做出的更改。
 * (这段其实我也不是很明白~)
 *
 * 注意:
 * 1. 不要在处理事件的时候使用{@link PathChildrenCache}中的数据，详细信息查看{@link PathChildrenCacheEvent}的类注释。
 * 2. 在启动缓存之前注册监听器。
 *
 * 正确的方式：
 * 1. 只在需要的时候使用{@link #getCurrentData()} 或 {@link #getCurrentData(String)}，而不使用监听器。
 * 2. 只使用监听器。
 * 如果既使用{@link PathChildrenCache}获取当前数据，又使用事件处理器，一定要仔细分析。
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
    /** curator 客户端 */
    private final CuratorFramework client;
    /**
     * 要监测的路径(path to watch)
     */
    private final String path;
    /**
     * 该缓存所有操作的执行线程。负责拉取{@link #path}下的数据 和 处理事件。
     * {@link PathChildrenCache} 和 {@link TreeCache}都使用了线程池，
     * 目的是相同的，一个节点下的缓存事件可能很多，避免阻塞 main-EventThread(数据更新 和 事件分发)。
     * 它包装的{@link ExecutorService}必须是单线程的线程池，否则可能导致数据不一致的情况，它需要保证操作和事件的顺序！
     * (共享线程池/指定线程池时一定要注意，必须是单线程的线程池。)
     */
    private final CloseableExecutorService executorService;
    /**
     * 是否缓存节点数据.
     * - 默认缓存。
     * - why? 一般来说我们使用Cache就是为了缓存数据的。
     */
    private final boolean cacheData;
    /** 数据是否是进行了压缩 */
    private final boolean dataIsCompressed;
    /**
     * 该缓存上的所有监听器，需要在调用{@link #start()}之前注册监听器。
     * why?
     * 如果你基于{@link #currentData}的检查结果决定是否进行监听，当你注册监听器的时候，你的检查结果可能是无效的！（无法控制main-thread更新data）
     */
    private final ListenerContainer<PathChildrenCacheListener> listeners = new ListenerContainer<PathChildrenCacheListener>();
    /**
     * 节点下数据的本地缓存。
     * 重要的事情说三遍：该数据由 main-EventThread进行更新！该数据由 main-EventThread进行更新！该数据由 main-EventThread进行更新！
     * 其它线程如果仅仅是偶尔需要以下当前的最新数据，那么可以使用{@link #getCurrentData()}。
     * 如果要处理事件，那么最好不要使用{@link #getCurrentData()}。
     */
    private final ConcurrentMap<String, ChildData> currentData = Maps.newConcurrentMap();
    /**
     * 节点的初始化数据。
     * 在需要处理初始化完成事件时，{@link AtomicReference#get()}是不为null的。
     */
    private final AtomicReference<Map<String, ChildData>> initialSet = new AtomicReference<Map<String, ChildData>>();
    /**
     * 待执行的操作结果。
     * Q: 为啥用ConcurrentMap的Set视图，而不是{@link java.util.concurrent.ConcurrentLinkedQueue}呢？
     * A: 当操作类型和路径相同的时候，这些操作将是等价的。比如：拉取同一个节点的数据， 拉取同一个节点的所有子节点。
     *
     * Q: 那会不会产生某些奇怪的问题呢？
     * A: TODO
     *
     * (ConcurrentMap不保证添加操作的顺序，此外有相同操作时，后面的操作无效)
     * (可能大多数情况下都并不需要这样的优化，对安全性造成的影响不是那么容易分析的)
     */
    private final Set<Operation> operationsQuantizer = Sets.newSetFromMap(Maps.<Operation, Boolean>newConcurrentMap());
    /**
     * 缓存的生命周期标识。
     */
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);

    /** 确保容器节点存在的辅助类(这个类单线程使用没问题，多线程下似乎有bug) */
    private final EnsureContainers ensureContainers;

    private enum State
    {
        /** 刚创建，还未启动的状态 */
        LATENT,
        /** 已启动状态 */
        STARTED,
        /** 已关闭状态 */
        CLOSED
    }

    /**
     * 空的childData，占位符
     */
    private static final ChildData NULL_CHILD_DATA = new ChildData("/", null, null);

    /**
     * 是否使用 {@link CuratorFramework#checkExists()}进行监听，默认false。
     * 因为使用{@link CuratorFramework#checkExists()}无法获取节点数据，只有不缓存节点数据时才有意义。
     */
    private static final boolean USE_EXISTS = Boolean.getBoolean("curator-path-children-cache-use-exists");

    /** 观察子节点增删事件的watcher */
    private volatile Watcher childrenWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            // 有子节点增删，拉取最新的子节点数据
            offerOperation(new RefreshOperation(PathChildrenCache.this, RefreshMode.STANDARD));
        }
    };

    /** 获取节点数据时，使用的watcher，用于监听节点删除和更新事件 */
    private volatile Watcher dataWatcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            try
            {
                if ( event.getType() == Event.EventType.NodeDeleted )
                {
                    // 监测到节点删除，删除本地缓存数据，并派发一个节点删除事件
                    remove(event.getPath());
                }
                else if ( event.getType() == Event.EventType.NodeDataChanged )
                {
                    // 节点数据发生了改变，压入一个拉取数据操作(命令)，避免阻塞main-EventThread
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

    /** 用于测试rebuild的exchanger，通过exchanger唤醒测试线程 */
    @VisibleForTesting
    volatile Exchanger<Object> rebuildTestExchanger;

    /** 客户端连接事件监听器 */
    private volatile ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            // 处理连接状态
            handleStateChange(newState);
        }
    };

    /**
     * 默认的线程工厂，如果构造方法未指定处理事件的线程池，那么将创建一个单线程的线程池，并使用该工厂对象。
     * 建议在使用{@link PathChildrenCache}的时候，指定线程池（必须单线程的线程池），可以复用线程池，减少线程数。
     * 因为一般来说，该线程池其实没有太多活。
     */
    private static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("PathChildrenCache");

    /**
     * 废弃方法，不在注释。
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
     * 废弃方法，不在注释。
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
     *                  要监测的路径(要观察的路径)
     * @param cacheData if true, node contents are cached in addition to the stat
     *                  是否缓存数据，如果缓存数据，除了stat之后，节点的内容将会被缓存。
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)
    {
        // 注意：这里新创建了单线程的线程池。使用的是默认的线程工厂。
        this(client, path, cacheData, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(defaultThreadFactory), true));
    }

    /**
     * @param client        the client
     * @param path          path to watch
     *                      要监测的路径(要观察的路径)
     * @param cacheData     if true, node contents are cached in addition to the stat
     *                      是否缓存数据，如果缓存数据，除了stat之后，节点的内容将会被缓存。
     * @param threadFactory factory to use when creating internal threads
     *                      创建内部线程的工厂。创建的线程用于处理事件和拉取数据。
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, ThreadFactory threadFactory)
    {
        // 注意：这里新创建了单线程的线程池。
        this(client, path, cacheData, false, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     *                         要监测的路径(要观察的路径)
     * @param cacheData        if true, node contents are cached in addition to the stat
     *                         是否缓存数据，如果缓存数据，除了stat之后，节点的内容将会被缓存。
     * @param dataIsCompressed if true, data in the path is compressed
     *                         节点数据是否使用压缩格式，如果为true，表示节点数据是经过压缩的
     * @param threadFactory    factory to use when creating internal threads
     *                         创建内部线程的工厂。创建的线程用于处理事件和拉取数据。
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, ThreadFactory threadFactory)
    {
        // 注意：这里新创建了单线程的线程池。
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(Executors.newSingleThreadExecutor(threadFactory), true));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     *                         要监测的路径(要观察的路径)
     * @param cacheData        if true, node contents are cached in addition to the stat
     *                         是否缓存数据，如果缓存数据，除了stat之后，节点的内容将会被缓存。
     * @param dataIsCompressed if true, data in the path is compressed
     *                         节点数据是否使用压缩格式，如果为true，表示节点数据是经过压缩的
     * @param executorService  ExecutorService to use for the PathChildrenCache's background thread.
     *                         This service should be single threaded, otherwise the cache may see inconsistent results.
     *                         用于PathChildrenCache的后台线程，该{@link ExecutorService}必须是单线程的，否则可能出现不一致的结果。
     *                         (需要保证操作和事件的顺序)
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, final ExecutorService executorService)
    {
        // 这里没有创建线程池，且线程池不会随着Cache的关闭而关闭
        this(client, path, cacheData, dataIsCompressed, new CloseableExecutorService(executorService));
    }

    /**
     * @param client           the client
     * @param path             path to watch
     *                         要监测的路径(要观察的路径)
     * @param cacheData        if true, node contents are cached in addition to the stat
     *                         是否缓存数据，如果缓存数据，除了stat之后，节点的内容将会被缓存。
     * @param dataIsCompressed if true, data in the path is compressed
     *                         节点数据是否使用压缩格式，如果为true，表示节点数据是经过压缩的
     * @param executorService  Closeable ExecutorService to use for the PathChildrenCache's background thread.
     *                         This service should be single threaded, otherwise the cache may see inconsistent results.
     *                         用于PathChildrenCache的后台线程，被包装的{@link ExecutorService}必须是单线程的，否则可能出现不一致的结果。
     *                         (需要保证操作和事件的顺序)
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
     * 使用默认的启动模式启动缓存。
     * 缓存并不是自动启动的，你必须调用该start方法启动缓存。
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        start(StartMode.NORMAL);
    }

    /**
     * 废弃方法，不在注释。
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
     * 启动缓存的方式(初始填充缓存的方式)。
     *
     * Method of priming cache on {@link PathChildrenCache#start(StartMode)}
     */
    public enum StartMode
    {
        /**
         * 普通模式。
         * 缓存将在后台装填好初始化值。当前存在的节点和新增的节点，都会发布事件。
         * eg：启动时，节点下已有子节点存在，初始拉取这些子节点时，会抛出事件。
         *
         * The cache will be primed (in the background) with initial values.
         * Events for existing and new nodes will be posted.
         */
        NORMAL,

        /**
         * 构建初始缓存模式。
         * 缓存将会在启动时同步填充初始缓存值。在{@link PathChildrenCache#start(StartMode)}返回之前，
         * 将会调用{@link PathChildrenCache#rebuild()}以获得该节点的初始视图。
         * （初始节点的拉取不会抛出事件）
         *
         * The cache will be primed (in the foreground) with initial values.
         * {@link PathChildrenCache#rebuild()} will be called before
         * the {@link PathChildrenCache#start(StartMode)} method returns
         * in order to get an initial view of the node.
         */
        BUILD_INITIAL_CACHE,

        /**
         * 报告初始化完成事件模式。
         * 缓存将在后台装填好初始化值，初始节点的拉取不会抛出事件，但是在初始缓存构建完成时会抛出一个
         * {@link PathChildrenCacheEvent.Type#INITIALIZED}事件。
         *
         * After cache is primed with initial values (in the background) a
         * {@link PathChildrenCacheEvent.Type#INITIALIZED} will be posted.
         */
        POST_INITIALIZED_EVENT
    }

    /**
     * 按照指定模式启动缓存，缓存并不会自动启动。你必须主动调用该方法启动缓存。
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @param mode Method for priming the cache 初始填充缓存的方式
     * @throws Exception errors
     */
    public void start(StartMode mode) throws Exception
    {
        // 通过原子变量，保护下面的代码块，只允许启动一次。
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");
        mode = Preconditions.checkNotNull(mode, "mode cannot be null");

        client.getConnectionStateListenable().addListener(connectionStateListener);

        switch ( mode )
        {
            case NORMAL:
            {
                // 以普通方式启动缓存时，在后台拉取所有节点数据
                offerOperation(new RefreshOperation(this, RefreshMode.STANDARD));
                break;
            }

            case BUILD_INITIAL_CACHE:
            {
                // 以构建初始缓存模式启动缓存，会以阻塞(同步的)的方式先拉取一次节点数据到缓存，然后后台再压入一个拉取数据操作。
                // 好处是方便，可以同步的方式获取当前数据。缺点是会多拉取一次数据。（rebuild不会注册watcher）
                rebuild();
                break;
            }

            case POST_INITIALIZED_EVENT:
            {
                // 以报告初始化完成事件模式启动缓存，在后台拉取所有节点数据，拉取完之后，会抛出一个init事件，可以获得初始化完成时的数据。
                initialSet.set(Maps.<String, ChildData>newConcurrentMap());
                offerOperation(new RefreshOperation(this, RefreshMode.POST_INITIALIZED));
                break;
            }
        }
    }

    /**
     * 注意：这是一个阻塞的方法。
     * 通过查询所有需要的数据完全重建内部缓存，而不生成任何事件发送给事件监听器(listeners)。
     * （刚方法在运行时不要随便调用，否则可能导致事件处理器得到的结果与缓存不一致。
     *  eg: rebuild清除了缓存，又收到了节点删除事件{@link #remove(String)},
     *      会导致无法抛出{@link PathChildrenCacheEvent.Type#CHILD_REMOVED}事件。
     * ）
     *
     * NOTE: this is a BLOCKING method. Completely rebuild the internal cache by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @throws Exception errors
     */
    public void rebuild() throws Exception
    {
        // 确保executor还未关闭(不过rebuild也用不上executor啊)
        Preconditions.checkState(!executorService.isShutdown(), "cache has been closed");

        // 确保路径存在(预构建)
        ensurePath();

        // 清除本地缓存
        clear();

        // 获取当前节点下的子节点们
        List<String> children = client.getChildren().forPath(path);
        for ( String child : children )
        {
            // 构建完整路径
            String fullPath = ZKPaths.makePath(path, child);
            // 内部构建某一个子节点(选择拉取数据的方式)
            internalRebuildNode(fullPath);

            // 唤醒测试组件
            if ( rebuildTestExchanger != null )
            {
                rebuildTestExchanger.exchange(new Object());
            }
        }

        // 压入一个refresh操作，这是有必要的，因为在重建缓存期间可能发生任何类型的更新。
        // 要保持本地缓存与远程的一致性，需要对远程节点进行监听，而rebuild本身是不监听节点的，
        // this is necessary so that any updates that occurred while rebuilding are taken
        offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
    }

    /**
     * 重建某个节点。
     * 注意：该方法是一个阻塞方法。通过查询所有需要的数据重建给定的节点缓存，并且不会生成任何事件发送给监听者。
     *
     * NOTE: this is a BLOCKING method. Rebuild the internal cache for the given node by querying
     * for all needed data WITHOUT generating any events to send to listeners.
     *
     * @param fullPath full path of the node to rebuild
     * @throws Exception errors
     */
    public void rebuildNode(String fullPath) throws Exception
    {
        // 检查是否是当前path下的节点
        Preconditions.checkArgument(ZKPaths.getPathAndNode(fullPath).getPath().equals(path), "Node is not part of this cache: " + fullPath);
        // 检查executorService是否关闭
        Preconditions.checkState(!executorService.isShutdown(), "cache has been closed");

        // 确保父节点存在
        ensurePath();
        // 获取该节点数据
        internalRebuildNode(fullPath);

        // 压入一个刷新操作，
        // this is necessary so that any updates that occurred while rebuilding are taken
        // have to rebuild entire tree in case this node got deleted in the interim
        offerOperation(new RefreshOperation(this, RefreshMode.FORCE_GET_DATA_AND_STAT));
    }

    /**
     * 关闭缓存
     *
     * Close/end the cache
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        if ( state.compareAndSet(State.STARTED, State.CLOSED) )
        {
            // 原子变量保护，只有started状态能切换到closed状态，释放各种资源

            client.getConnectionStateListenable().removeListener(connectionStateListener);
            listeners.clear();
            executorService.close();
            // 取消watcher
            client.clearWatcherReferences(childrenWatcher);
            client.clearWatcherReferences(dataWatcher);

            // 将引用显式置为null可以帮助gc(释放引用的对象)
            // TODO
            // This seems to enable even more GC - I'm not sure why yet - it
            // has something to do with Guava's cache and circular references
            connectionStateListener = null;
            childrenWatcher = null;
            dataWatcher = null;
        }
    }

    /**
     * 返回监听器的容器。
     * 使用{@link ListenerContainer}可以提供统一的管理，实现重用。
     * Return the cache listenable
     *
     * @return listenable
     */
    public ListenerContainer<PathChildrenCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * 返回当前数据。
     * 没有任何准确性保证，这只是数据的最新视图。数据按排序顺序返回。
     *
     * 再次提醒：
     * 1. 不要在处理事件的时候使用{@link PathChildrenCache}获取数据。
     * 2. 如果根据当前数据决定是否添加监听器，可能会导致错误（检查结果失效）。
     *
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
     * 返回给定完整路径的当前数据。
     * 没有任何准确性保证，这只是数据的最新视图，如果该节点当前不存在，则返回null。
     *
     * 警告：如果不清楚带来的影响，不要轻易调用。
     * eg：
     * 1. 不要在处理事件的时候使用{@link PathChildrenCache}获取数据。
     * 2. 如果根据当前数据决定是否添加监听器，可能会导致错误（检查结果失效）。
     *
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
     * 您可以清除节点的缓存数据字节作为内存优化一种方式。
     * 对该节点的{@link ChildData#getData()} 的后续调用将返回<code>null</code>。
     *
     * 警告：如果不清楚带来的影响，不要轻易调用。
     *
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath the path of the node to clear
     */
    public void clearDataBytes(String fullPath)
    {
        // -1 强制清除
        clearDataBytes(fullPath, -1);
    }

    /**
     * 您可以清除节点的缓存数据字节作为内存优化一种方式。
     * 对该节点的{@link ChildData#getData()} 的后续调用将返回<code>null</code>。
     *
     * 警告：如果不清楚带来的影响，不要轻易调用。
     *
     * As a memory optimization, you can clear the cached data bytes for a node. Subsequent
     * calls to {@link ChildData#getData()} for this node will return <code>null</code>.
     *
     * @param fullPath  the path of the node to clear
     *                  要清除数据的路径
     * @param ifVersion if non-negative, only clear the data if the data's version matches this version
     *                  如果非负，仅当该版本号与数据的版本号匹配的还是才会清除节点数据。
     * @return true if the data was cleared
     *          如果清除成功则返回true，否则返回false
     */
    public boolean clearDataBytes(String fullPath, int ifVersion)
    {
        ChildData data = currentData.get(fullPath);
        if ( data != null )
        {
            // 指定版本为负数则强制删除，如果版本号与数据的版本匹配则进行删除(用在竞争操作中)
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
     * 清除缓存的当且数据，并且不会生成任何事件给监听器，并以标准模式重新拉取数据。
     *
     * 警告：如果不清楚带来的影响，不要轻易调用。（会对监听器造成影响）
     *
     * Clear out current data and begin a new query on the path
     *
     * @throws Exception errors
     */
    public void clearAndRefresh() throws Exception
    {
        currentData.clear();
        // 重新拉取最新数据
        offerOperation(new RefreshOperation(this, RefreshMode.STANDARD));
    }

    /**
     * 清除缓存当前数据，并且不生成任何事件给监听器。
     * (该方法用在开始新一轮的查询之前，虽然是public，但是外部最好不要调用)
     *
     * Clears the current data without beginning a new query and without generating any events
     * for listeners.
     */
    public void clear()
    {
        currentData.clear();
    }

    /** 缓存刷新模式 */
    enum RefreshMode
    {
        /**
         * 标准模式
         */
        STANDARD,
        /**
         * 强制获取节点数据和状态
         */
        FORCE_GET_DATA_AND_STAT,
        /**
         * 拉取完成之后，抛出一个初始化完成事件
         */
        POST_INITIALIZED
    }

    /**
     * 刷新缓存
     * @param mode 刷新缓存的模式
     * @throws Exception zookeeper errors
     */
    void refresh(final RefreshMode mode) throws Exception
    {
        // 确保容器节点存在(创建必要节点)
        ensurePath();
        // 拉取children操作完成时的回调
        final BackgroundCallback callback = new BackgroundCallback()
        {
            @Override
            public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
            {
                if (PathChildrenCache.this.state.get().equals(State.CLOSED)) {
                    // This ship is closed, don't handle the callback
                    // 缓存已关闭，不必处理该回调。（小船已翻）
                    return;
                }
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    // 拉取操作成功，处理拉取到的子节点信息
                    processChildren(event.getChildren(), mode);
                }
            }
        };

        client.getChildren().usingWatcher(childrenWatcher).inBackground(callback).forPath(path);
    }

    /**
     * 通知监听者们
     * @param event 要通知的事件
     */
    void callListeners(final PathChildrenCacheEvent event)
    {
        // 这是旧的java版本 不支持lambda表达式的时候写的，读起来可能难受点
        listeners.forEach
            (
                new Function<PathChildrenCacheListener, Void>()
                {
                    @Override
                    public Void apply(PathChildrenCacheListener listener)
                    {
                        try
                        {
                            // 执行监听器的回调方法
                            listener.childEvent(client, event);
                        }
                        catch ( Exception e )
                        {
                            // 检查是否需要恢复中断
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
            // 始终使用getData()代替exists()以避免留下不必要的watcher，这些watcher是一种资源泄漏
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
     * 处理异常。
     * 默认处理异常的方式仅仅是记录日志。
     *
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void handleException(Throwable e)
    {
        log.error("", e);
    }

    /** 确保路径存在 */
    protected void ensurePath() throws Exception
    {
        ensureContainers.ensure();
    }

    /**
     * 删除本地缓的某个节点数据
     * @param fullPath 删除的节点数据
     */
    @VisibleForTesting
    protected void remove(String fullPath)
    {
        ChildData data = currentData.remove(fullPath);
        if ( data != null )
        {
            // 如果节点在当前数据中，那么则触发节点删除事件
            offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, data)));
        }

        Map<String, ChildData> localInitialSet = initialSet.get();
        if ( localInitialSet != null )
        {
            localInitialSet.remove(fullPath);
            maybeOfferInitializedEvent(localInitialSet);
        }
    }

    /**
     * 内部构建某一个子节点
     * @param fullPath 子节点完整路径
     * @throws Exception zookeeper errors
     */
    private void internalRebuildNode(String fullPath) throws Exception
    {
        if ( cacheData )
        {
            // 需要缓存数据，使用getData
            try
            {
                Stat stat = new Stat();
                // 拉取数据存入缓存，rebuild不抛出事件，因此不做缓存值与最新值的比较。
                byte[] bytes = dataIsCompressed ? client.getData().decompressed().storingStatIn(stat).forPath(fullPath) : client.getData().storingStatIn(stat).forPath(fullPath);
                currentData.put(fullPath, new ChildData(fullPath, stat, bytes));
            }
            catch ( KeeperException.NoNodeException ignore )
            {
                // 即使调用方法前，节点存在。当发起拉取数据的时候，节点又可能是不存在的。
                // 节点不存在时，删除该节点的数据。 rebuild方法不抛出事件，因此不做返回值检测
                // node no longer exists - remove it
                currentData.remove(fullPath);
            }
        }
        else
        {
            // 不缓存数据，只缓存状态，使用checkExists，拉取最新的stat到缓存。（逻辑基本同上面）
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

    /**
     * 处理连接状态改变事件
     * @param newState 当前状态
     */
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
                // 连接或重连之后会重新拉取最新数据
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

    /**
     * 处理拉取到的子节点信息 （调用该方法是 main-EventThread）
     * @param children 节点当前的最小子节点信息
     * @param mode 拉取节点的模式
     * @throws Exception zookeeper errors
     */
    private void processChildren(List<String> children, RefreshMode mode) throws Exception
    {
        // removedNodes 用于统计已删除的节点， = 缓存 - 最新节点信息
        Set<String> removedNodes = Sets.newHashSet(currentData.keySet());
        for ( String child : children ) {
            removedNodes.remove(ZKPaths.makePath(path, child));
        }
        // 删除本地缓存
        for ( String fullPath : removedNodes )
        {
            remove(fullPath);
        }
        // 拉取子节点最新数据
        for ( String name : children )
        {
            String fullPath = ZKPaths.makePath(path, name);
            if ( (mode == RefreshMode.FORCE_GET_DATA_AND_STAT) || !currentData.containsKey(fullPath) )
            {
                // 刷新模式为强制拉取节点数据 或 这是一个新节点的情况下，拉取子节点的数据
                getDataAndStat(fullPath);
            }
            // 使用占位符填充到初始缓存信息中，用于标记正在尝试拉取的节点数
            updateInitialSet(name, NULL_CHILD_DATA);
        }
        // 判断是否需要抛出初始化完成事件
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
                // 不存在旧数据，这是一个新数据，抛出一个CHILD_ADDED事件
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, data)));
            }
            else if ( previousData.getStat().getVersion() != stat.getVersion() )
            {
                // 存在旧数据，但是版本号并不同，抛出一个CHILD_UPDATED事件。
                offerOperation(new EventOperation(this, new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data)));
            }
            // 尝试压入初始缓存集合中
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

    /**
     * 判断是否初始化完成了
     * @param localInitialSet 缓存的某一时刻值，减少volatile读
     * @return true/false
     */
    private boolean hasUninitialized(Map<String, ChildData> localInitialSet)
    {
        if ( localInitialSet == null )
        {
            // 不需要处理缓存初始化完成事件
            return false;
        }
        // 统计未完成初始化的节点数
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
        // 所有节点都已完成初始化
        return (uninitializedChildren.size() != 0);
    }

    /**
     * 将一个操作提交给{@link #executorService}
     * @param operation 要提交的操作
     */
    void offerOperation(final Operation operation)
    {
        // 压入本地集合，判断是否是一个有效的新操作，说实话可能导致一些潜在的风险
        if ( operationsQuantizer.add(operation) )
        {
            // 这是一个新操作，需要提交给{@link #executorService}
            submitToExecutor
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        // 这里是executor所在的线程。（executor是一个单线程的executor）
                        try
                        {
                            // 从本地集合中删除，使得能再放入相同的操作
                            operationsQuantizer.remove(operation);
                            // 真正执行操作
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
                            // 恢复中断
                            Thread.currentThread().interrupt();
                        }
                        catch ( Exception e )
                        {
                            // 怎么又判断一次中断异常？？？
                            ThreadUtils.checkInterrupted(e);
                            // 处理异常
                            handleException(e);
                        }
                    }
                }
            );
        }
    }

    /**
     * 提交一个任务到executor。
     * 原注释：
     * 该方法是同步的，因为必须检查状态判断缓存是否仍然在启动中。
     * 如果不检查状态，则可能出现一些竞争条件（watcher产生的）。
     * 即使在缓存关闭之后，该方法仍然可能被这些watcher调用(main-EventThread),因为close方法并不真正的禁用这些watcher。
     *
     * 在没有竞争的情况下，同步的消耗应该是很小的。因为一般来说，该方法仅由 ZK客户端线程调用（main-EventThread）,
     * 并且只有在close方法和zk客户端线程update数据并行执行的时候才会产生竞争，这是我们期望保护的确切状态。
     * --
     * 点评：这代码有bug，这里的synchronized实际上没有用的！因为未给close加锁！它并不能阻止state的状态改变！
     * 类似的还有{@link EnsureContainers#ensure()}
     *
     * 搞得我都有点懵逼了，遇见好几次这样的代码了，搞得都怀疑是我知识有问题还是这代码有问题了。。。。
     * close方法不是同步方法，两个方法产生了什么竞争？？？
     *
     * 检查到state为{@link State#CLOSED}，然后执行特定代码没有问题，因为切换到CLOSED以后，state不会再改变，这个先检查后执行的检查结果不会无效。
     * 但是检查到state为{@link State#STARTED},再执行特定代码是不科学的，因为state可能会被切换到{@link State#CLOSED}状态，这个先检查后执行的
     * 检查结果是会失效的！ 一厢情愿的自己获取锁，是没有用的，还应该给close方法加锁，使得这里的检查结果不会发生改变！
     *
     * {@link EnsureContainers} 也有这样的问题，{@link AtomicReference}这类原子变量并不能保证简单的先检查后执行的互斥！
     *
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
            // 感觉和EnsureContainers有一样的问题，这里的锁并不能阻塞close方法执行！close方法也不能阻止这里执行！
            // 在这里的时候state可能是close！因为close方法并没有获得锁！
            // 无论是确保提交的时候是started状态，还是说executor为关闭，这代码都不能保证啊
            executorService.submit(command);
        }
    }
}
