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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.curator.framework.listen.Listenable;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.curator.utils.PathUtils.validatePath;

/**
 * 一个尝试将指定zk路径下的所有子节点数据保存下来的实用本地缓存。
 * 该类将会watch指定的zk路径，响应子节点的增删改事件，并拉取所有的数据等。
 * 你可以注册一个{@link TreeCacheListener}，当产生改变的时候，listener将会得到通知。
 *
 * <b>非常重要</b>
 * 该类不能保持事务同步！该类的使用者必须为 假正(FP) 和假负(FN)作出准备。此外，在更新数据时始终使用版本号，避免覆盖其它进程做出的更改。
 *
 *
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 * <p></p>
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 */
public class TreeCache implements Closeable
{
    private static final Logger LOG = LoggerFactory.getLogger(TreeCache.class);
    private final boolean createParentNodes;
    private final TreeCacheSelector selector;

    // ---------------------------------- Builder 模式，引导创建TreeCache -------------------------
    public static final class Builder
    {
        private final CuratorFramework client;
        /** 要缓存的目录节点 */
        private final String path;
        /** 是否缓存数据，默认true。一般使用缓存就是为了获取节点数据的 */
        private boolean cacheData = true;
        /** 节点数据是否进行了压缩 */
        private boolean dataIsCompressed = false;
        /**
         * TreeCacheListener的执行环境，由它来通知监听器和后台拉取数据。
         * 注意: {@link TreeCache}在关闭的时候 {@link #close()}会关闭Executor!!!!没有选项。
         * 因此传入的{@link ExecutorService}最好是的{@link ExecutorService}.
         */
        private ExecutorService executorService = null;
        /** 最大缓存深度，树的深度，默认Max即缓存所有节点 */
        private int maxDepth = Integer.MAX_VALUE;
        /** 是否创建父节点，也就是说要缓存的节点的父节点可以不存在 */
        private boolean createParentNodes = false;
        /**
         * 缓存节点选择器，用于判断哪些节点缓存，哪些节点不缓存。
         */
        private TreeCacheSelector selector = new DefaultTreeCacheSelector();

        private Builder(CuratorFramework client, String path)
        {
            this.client = checkNotNull(client);
            this.path = validatePath(path);
        }

        /**
         * Builds the {@link TreeCache} based on configured values.
         */
        public TreeCache build()
        {
            ExecutorService executor = executorService;
            if ( executor == null )
            {
                // 注意：为了保证事件的顺序，必须是单线程的executor！！！
                executor = Executors.newSingleThreadExecutor(defaultThreadFactory);
            }
            return new TreeCache(client, path, cacheData, dataIsCompressed, maxDepth, executor, createParentNodes, selector);
        }

        /**
         * 设置是否缓存每个节点的数据。默认为true。
         *
         * Sets whether or not to cache byte data per node; default {@code true}.
         */
        public Builder setCacheData(boolean cacheData)
        {
            this.cacheData = cacheData;
            return this;
        }

        /**
         * 设置节点数据是否进行了压缩，默认false。
         * Sets whether or to decompress node data; default {@code false}.
         */
        public Builder setDataIsCompressed(boolean dataIsCompressed)
        {
            this.dataIsCompressed = dataIsCompressed;
            return this;
        }

        /**
         * 设置发布事件的executor (listener的执行环境)。如果不设置的话，会创建一个默认的executor。
         *
         * Sets the executor to publish events; a default executor will be created if not specified.
         */
        public Builder setExecutor(ThreadFactory threadFactory)
        {
            return setExecutor(Executors.newSingleThreadExecutor(threadFactory));
        }

        /**
         * 注意：为了保证事件的顺序，必须是单线程的executor！！！
         * 这个{@link Deprecated}是译者我(wjybxx)加的，我建议使用{@link #setExecutor(ThreadFactory)}，
         * 否则容易忽略两个问题：
         * 1. ExecutorService 必须是单线程的，否则无法保证事件的顺序，无法保证数据的一致性。
         * 2. ExecutorService 会在TreeCache关闭的时候被关闭！！！如果外部也使用该ExecutorService，会出现问题。
         *
         * Sets the executor to publish events; a default executor will be created if not specified.
         */
        @Deprecated
        public Builder setExecutor(ExecutorService executorService)
        {
            this.executorService = checkNotNull(executorService);
            return this;
        }

        /**
         * 设置缓存的最大探索/观察深度。
         * 如果{@code maxDepth} 为 {@code 0}，则表示只缓存指定的根节点，就像{@link NodeCache}。
         * 如果{@code maxDepth} 为 {@code 1}，则表示只缓存根节点的直接子节点，就像{@link PathChildrenCache}。
         * 默认值为{@code Integer.MAX_VALUE}，表示缓存缓存指定节点的所有子节点。
         *
         * Sets the maximum depth to explore/watch.  A {@code maxDepth} of {@code 0} will watch only
         * the root node (like {@link NodeCache}); a {@code maxDepth} of {@code 1} will watch the
         * root node and its immediate children (kind of like {@link PathChildrenCache}.
         * Default: {@code Integer.MAX_VALUE}
         */
        public Builder setMaxDepth(int maxDepth)
        {
            this.maxDepth = maxDepth;
            return this;
        }

        /**
         * 设置是否自动创建缓存节点的父节点。
         * 默认情况下，TreeCache不会自动创建要缓存的节点的父节点，可以使用该方法改变该行为。
         * 注意：父节点会被创建为容器节点。
         *
         * By default, TreeCache does not auto-create parent nodes for the cached path. Change
         * this behavior with this method. NOTE: parent nodes are created as containers
         *
         * @param createParentNodes true to create parent nodes
         * @return this for chaining
         */
        public Builder setCreateParentNodes(boolean createParentNodes)
        {
            this.createParentNodes = createParentNodes;
            return this;
        }

        /**
         * 设置treeCache的子节点选择器，用于选择哪些节点需要缓存，哪些节点不需要缓存。
         * 默认使用{@link DefaultTreeCacheSelector} (缓存所有节点)，可以通过该方法改变。
         *
         * By default, {@link DefaultTreeCacheSelector} is used. Change the selector here.
         *
         * @param selector new selector
         * @return this for chaining
         */
        public Builder setSelector(TreeCacheSelector selector)
        {
            this.selector = selector;
            return this;
        }
    }

    /**
     * 使用指定的客户端和路径创建一个Builder，以更好的配置选项.
     * (用于引导创建{@link TreeCache})
     *
     * 如果client是一个命名空间，那么TreeCache上的所有操作结果都将以名称空间为单位，包括所有已发布的事件。
     * 给定的路径是TreeCache观察和探索的根节点。如果给定路径中不存在子节点，则TreeCache最初将为空。
     *
     * Create a TreeCache builder for the given client and path to configure advanced options.
     * <p/>
     * If the client is namespaced, all operations on the resulting TreeCache will be in terms of
     * the namespace, including all published events.  The given path is the root at which the
     * TreeCache will watch and explore.  If no node exists at the given path, the TreeCache will
     * be initially empty.
     *
     * @param client the client to use; may be namespaced
     *               使用的curator客户端，也可能是一个命名空间。
     * @param path   the path to the root node to watch/explore; this path need not actually exist on
     *               the server
     *               要缓存的根节点路径，该节点并不需要在服务器上真实存在。
     * @return a new builder
     */
    public static Builder newBuilder(CuratorFramework client, String path)
    {
        return new Builder(client, path);
    }

    // -------------------------------------------------- TreeNode ---------------------------------------------------

    private enum NodeState
    {
        PENDING, LIVE, DEAD
    }

    private static final AtomicReferenceFieldUpdater<TreeNode, NodeState> nodeStateUpdater =
            AtomicReferenceFieldUpdater.newUpdater(TreeNode.class, NodeState.class, "nodeState");

    private static final AtomicReferenceFieldUpdater<TreeNode, ChildData> childDataUpdater =
            AtomicReferenceFieldUpdater.newUpdater(TreeNode.class, ChildData.class, "childData");

    private static final AtomicReferenceFieldUpdater<TreeNode, ConcurrentMap> childrenUpdater =
            AtomicReferenceFieldUpdater.newUpdater(TreeNode.class, ConcurrentMap.class, "children");

    private final class TreeNode implements Watcher, BackgroundCallback
    {

        volatile NodeState nodeState = NodeState.PENDING;
        volatile ChildData childData;
        final TreeNode parent;
        final String path;
        volatile ConcurrentMap<String, TreeNode> children;
        final int depth;

        TreeNode(String path, TreeNode parent)
        {
            this.path = path;
            this.parent = parent;
            this.depth = parent == null ? 0 : parent.depth + 1;
        }

        private void refresh() throws Exception
        {
            if ((depth < maxDepth) && selector.traverseChildren(path))
            {
                outstandingOps.addAndGet(2);
                doRefreshData();
                doRefreshChildren();
            } else {
                refreshData();
            }
        }

        private void refreshChildren() throws Exception
        {
            if ((depth < maxDepth) && selector.traverseChildren(path))
            {
                outstandingOps.incrementAndGet();
                doRefreshChildren();
            }
        }

        private void refreshData() throws Exception
        {
            outstandingOps.incrementAndGet();
            doRefreshData();
        }

        private void doRefreshChildren() throws Exception
        {
            client.getChildren().usingWatcher(this).inBackground(this).forPath(path);
        }

        private void doRefreshData() throws Exception
        {
            if ( dataIsCompressed )
            {
                client.getData().decompressed().usingWatcher(this).inBackground(this).forPath(path);
            }
            else
            {
                client.getData().usingWatcher(this).inBackground(this).forPath(path);
            }
        }

        void wasReconnected() throws Exception
        {
            refresh();
            ConcurrentMap<String, TreeNode> childMap = children;
            if ( childMap != null )
            {
                for ( TreeNode child : childMap.values() )
                {
                    child.wasReconnected();
                }
            }
        }

        void wasCreated() throws Exception
        {
            refresh();
        }

        void wasDeleted() throws Exception
        {
            ChildData oldChildData = childDataUpdater.getAndSet(this, null);
            client.clearWatcherReferences(this);
            ConcurrentMap<String, TreeNode> childMap = childrenUpdater.getAndSet(this,null);
            if ( childMap != null )
            {
                ArrayList<TreeNode> childCopy = new ArrayList<TreeNode>(childMap.values());
                childMap.clear();
                for ( TreeNode child : childCopy )
                {
                    child.wasDeleted();
                }
            }

            if ( treeState.get() == TreeState.CLOSED )
            {
                return;
            }

            NodeState oldState = nodeStateUpdater.getAndSet(this, NodeState.DEAD);
            if ( oldState == NodeState.LIVE )
            {
                publishEvent(TreeCacheEvent.Type.NODE_REMOVED, oldChildData);
            }

            if ( parent == null )
            {
                // Root node; use an exist query to watch for existence.
                client.checkExists().usingWatcher(this).inBackground(this).forPath(path);
            }
            else
            {
                // Remove from parent if we're currently a child
                ConcurrentMap<String, TreeNode> parentChildMap = parent.children;
                if ( parentChildMap != null )
                {
                    parentChildMap.remove(ZKPaths.getNodeFromPath(path), this);
                }
            }
        }

        @Override
        public void process(WatchedEvent event)
        {
            LOG.debug("process: {}", event);
            try
            {
                switch ( event.getType() )
                {
                case NodeCreated:
                    Preconditions.checkState(parent == null, "unexpected NodeCreated on non-root node");
                    wasCreated();
                    break;
                case NodeChildrenChanged:
                    refreshChildren();
                    break;
                case NodeDataChanged:
                    refreshData();
                    break;
                case NodeDeleted:
                    wasDeleted();
                    break;
                }
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        }

        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception
        {
            LOG.debug("processResult: {}", event);
            Stat newStat = event.getStat();
            switch ( event.getType() )
            {
            case EXISTS:
                Preconditions.checkState(parent == null, "unexpected EXISTS on non-root node");
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    nodeStateUpdater.compareAndSet(this, NodeState.DEAD, NodeState.PENDING);
                    wasCreated();
                }
                break;
            case CHILDREN:
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    ChildData oldChildData = childData;
                    if ( oldChildData != null && oldChildData.getStat().getMzxid() == newStat.getMzxid() )
                    {
                        // Only update stat if mzxid is same, otherwise we might obscure
                        // GET_DATA event updates.
                        childDataUpdater.compareAndSet(this, oldChildData, new ChildData(oldChildData.getPath(), newStat, oldChildData.getData()));
                    }

                    if ( event.getChildren().isEmpty() )
                    {
                        break;
                    }

                    ConcurrentMap<String, TreeNode> childMap = children;
                    if ( childMap == null )
                    {
                        childMap = Maps.newConcurrentMap();
                        if ( !childrenUpdater.compareAndSet(this, null, childMap) )
                        {
                            childMap = children;
                        }
                    }

                    // Present new children in sorted order for test determinism.
                    List<String> newChildren = new ArrayList<String>();
                    for ( String child : event.getChildren() )
                    {
                        if ( !childMap.containsKey(child) && selector.acceptChild(ZKPaths.makePath(path, child)) )
                        {
                            newChildren.add(child);
                        }
                    }

                    Collections.sort(newChildren);
                    for ( String child : newChildren )
                    {
                        String fullPath = ZKPaths.makePath(path, child);
                        TreeNode node = new TreeNode(fullPath, this);
                        if ( childMap.putIfAbsent(child, node) == null )
                        {
                            node.wasCreated();
                        }
                    }
                }
                else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    wasDeleted();
                }
                break;
            case GET_DATA:
                if ( event.getResultCode() == KeeperException.Code.OK.intValue() )
                {
                    ChildData toPublish = new ChildData(event.getPath(), newStat, event.getData());
                    ChildData oldChildData;
                    if ( cacheData )
                    {
                        oldChildData = childDataUpdater.getAndSet(this, toPublish);
                    }
                    else
                    {
                        oldChildData = childDataUpdater.getAndSet(this, new ChildData(event.getPath(), newStat, null));
                    }

                    boolean added;
                    if (parent == null) {
                        // We're the singleton root.
                        added = nodeStateUpdater.getAndSet(this, NodeState.LIVE) != NodeState.LIVE;
                    } else {
                        added = nodeStateUpdater.compareAndSet(this, NodeState.PENDING, NodeState.LIVE);
                        if (!added) {
                            // Ordinary nodes are not allowed to transition from dead -> live;
                            // make sure this isn't a delayed response that came in after death.
                            if (nodeState != NodeState.LIVE) {
                                return;
                            }
                        }
                    }

                    if ( added )
                    {
                        publishEvent(TreeCacheEvent.Type.NODE_ADDED, toPublish);
                    }
                    else
                    {
                        if ( oldChildData == null || oldChildData.getStat().getMzxid() != newStat.getMzxid() )
                        {
                            publishEvent(TreeCacheEvent.Type.NODE_UPDATED, toPublish);
                        }
                    }
                }
                else if ( event.getResultCode() == KeeperException.Code.NONODE.intValue() )
                {
                    wasDeleted();
                }
                break;
            default:
                // An unknown event, probably an error of some sort like connection loss.
                LOG.info(String.format("Unknown event %s", event));
                // Don't produce an initialized event on error; reconnect can fix this.
                outstandingOps.decrementAndGet();
                return;
            }

            if ( outstandingOps.decrementAndGet() == 0 )
            {
                if ( isInitialized.compareAndSet(false, true) )
                {
                    publishEvent(TreeCacheEvent.Type.INITIALIZED);
                }
            }
        }
    }

    private enum TreeState
    {
        LATENT,
        STARTED,
        CLOSED
    }

    /**
     * Tracks the number of outstanding background requests in flight. The first time this count reaches 0, we publish the initialized event.
     */
    private final AtomicLong outstandingOps = new AtomicLong(0);

    /**
     * Have we published the {@link TreeCacheEvent.Type#INITIALIZED} event yet?
     */
    private final AtomicBoolean isInitialized = new AtomicBoolean(false);

    /** 缓存对应的根节点{@link Builder#path} */
    private final TreeNode root;
    private final CuratorFramework client;
    /**
     * listener的执行环境。
     * 这里不得不说的一点是：调用{@link TreeCache#close()}的时候会关闭executor(都不给一个选项)。
     * 用的不是{@link CloseableExecutorService}。
     */
    private final ExecutorService executorService;
    private final boolean cacheData;
    private final boolean dataIsCompressed;
    private final int maxDepth;
    private final ListenerContainer<TreeCacheListener> listeners = new ListenerContainer<TreeCacheListener>();
    private final ListenerContainer<UnhandledErrorListener> errorListeners = new ListenerContainer<UnhandledErrorListener>();
    private final AtomicReference<TreeState> treeState = new AtomicReference<TreeState>(TreeState.LATENT);

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            handleStateChange(newState);
        }
    };

    static final ThreadFactory defaultThreadFactory = ThreadUtils.newThreadFactory("TreeCache");

    /**
     * Create a TreeCache for the given client and path with default options.
     * <p/>
     * If the client is namespaced, all operations on the resulting TreeCache will be in terms of
     * the namespace, including all published events.  The given path is the root at which the
     * TreeCache will watch and explore.  If no node exists at the given path, the TreeCache will
     * be initially empty.
     *
     * @param client the client to use; may be namespaced
     * @param path   the path to the root node to watch/explore; this path need not actually exist on
     *               the server
     * @see #newBuilder(CuratorFramework, String)
     */
    public TreeCache(CuratorFramework client, String path)
    {
        this(client, path, true, false, Integer.MAX_VALUE, Executors.newSingleThreadExecutor(defaultThreadFactory), false, new DefaultTreeCacheSelector());
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     * @param executorService  Closeable ExecutorService to use for the TreeCache's background thread
     * @param createParentNodes true to create parent nodes as containers
     * @param selector         the selector to use
     */
    TreeCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed, int maxDepth, final ExecutorService executorService, boolean createParentNodes, TreeCacheSelector selector)
    {
        this.createParentNodes = createParentNodes;
        this.selector = Preconditions.checkNotNull(selector, "selector cannot be null");
        this.root = new TreeNode(validatePath(path), null);
        this.client = Preconditions.checkNotNull(client, "client cannot be null");
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
        this.maxDepth = maxDepth;
        this.executorService = Preconditions.checkNotNull(executorService, "executorService cannot be null");
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @return this
     * @throws Exception errors
     */
    public TreeCache start() throws Exception
    {
        Preconditions.checkState(treeState.compareAndSet(TreeState.LATENT, TreeState.STARTED), "already started");
        if ( createParentNodes )
        {
            client.createContainers(root.path);
        }
        client.getConnectionStateListenable().addListener(connectionStateListener);
        if ( client.getZookeeperClient().isConnected() )
        {
            root.wasCreated();
        }
        return this;
    }

    /**
     * Close/end the cache.
     */
    @Override
    public void close()
    {
        if ( treeState.compareAndSet(TreeState.STARTED, TreeState.CLOSED) )
        {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            listeners.clear();
            executorService.shutdown();
            try
            {
                root.wasDeleted();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
        }
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public Listenable<TreeCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * Allows catching unhandled errors in asynchornous operations.
     *
     * TODO: consider making public.
     */
    @VisibleForTesting
    public Listenable<UnhandledErrorListener> getUnhandledErrorListenable()
    {
        return errorListeners;
    }

    private TreeNode find(String findPath)
    {
        PathUtils.validatePath(findPath);
        LinkedList<String> rootElements = new LinkedList<String>(ZKPaths.split(root.path));
        LinkedList<String> findElements = new LinkedList<String>(ZKPaths.split(findPath));
        while (!rootElements.isEmpty()) {
            if (findElements.isEmpty()) {
                // Target path shorter than root path
                return null;
            }
            String nextRoot = rootElements.removeFirst();
            String nextFind = findElements.removeFirst();
            if (!nextFind.equals(nextRoot)) {
                // Initial root path does not match
                return null;
            }
        }

        TreeNode current = root;
        while (!findElements.isEmpty()) {
            String nextFind = findElements.removeFirst();
            ConcurrentMap<String, TreeNode> map = current.children;
            if ( map == null )
            {
                return null;
            }
            current = map.get(nextFind);
            if ( current == null )
            {
                return null;
            }
        }
        return current;
    }

    /**
     * Return the current set of children at the given path, mapped by child name. There are no
     * guarantees of accuracy; this is merely the most recent view of the data.  If there is no
     * node at this path, {@code null} is returned.
     *
     * @param fullPath full path to the node to check
     * @return a possibly-empty list of children if the node is alive, or null
     */
    public Map<String, ChildData> getCurrentChildren(String fullPath)
    {
        TreeNode node = find(fullPath);
        if ( node == null || node.nodeState != NodeState.LIVE )
        {
            return null;
        }
        ConcurrentMap<String, TreeNode> map = node.children;
        Map<String, ChildData> result;
        if ( map == null )
        {
            result = ImmutableMap.of();
        }
        else
        {
            ImmutableMap.Builder<String, ChildData> builder = ImmutableMap.builder();
            for ( Map.Entry<String, TreeNode> entry : map.entrySet() )
            {
                TreeNode childNode = entry.getValue();
                ChildData childData = childNode.childData;
                // Double-check liveness after retreiving data.
                if ( childData != null && childNode.nodeState == NodeState.LIVE )
                {
                    builder.put(entry.getKey(), childData);
                }
            }
            result = builder.build();
        }

        // Double-check liveness after retreiving children.
        return node.nodeState == NodeState.LIVE ? result : null;
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no node at the given path,
     * {@code null} is returned.
     *
     * @param fullPath full path to the node to check
     * @return data if the node is alive, or null
     */
    public ChildData getCurrentData(String fullPath)
    {
        TreeNode node = find(fullPath);
        if ( node == null || node.nodeState != NodeState.LIVE )
        {
            return null;
        }
        ChildData result = node.childData;
        // Double-check liveness after retreiving data.
        return node.nodeState == NodeState.LIVE ? result : null;
    }

    private void callListeners(final TreeCacheEvent event)
    {
        listeners.forEach(new Function<TreeCacheListener, Void>()
        {
            @Override
            public Void apply(TreeCacheListener listener)
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
        });
    }

    /**
     * Send an exception to any listeners, or else log the error if there are none.
     */
    private void handleException(final Throwable e)
    {
        if ( errorListeners.size() == 0 )
        {
            LOG.error("", e);
        }
        else
        {
            errorListeners.forEach(new Function<UnhandledErrorListener, Void>()
            {
                @Override
                public Void apply(UnhandledErrorListener listener)
                {
                    try
                    {
                        listener.unhandledError("", e);
                    }
                    catch ( Exception e )
                    {
                        ThreadUtils.checkInterrupted(e);
                        LOG.error("Exception handling exception", e);
                    }
                    return null;
                }
            });
        }
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState )
        {
        case SUSPENDED:
            publishEvent(TreeCacheEvent.Type.CONNECTION_SUSPENDED);
            break;

        case LOST:
            isInitialized.set(false);
            publishEvent(TreeCacheEvent.Type.CONNECTION_LOST);
            break;

        case CONNECTED:
            try
            {
                root.wasCreated();
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
            break;

        case RECONNECTED:
            try
            {
                root.wasReconnected();
                publishEvent(TreeCacheEvent.Type.CONNECTION_RECONNECTED);
            }
            catch ( Exception e )
            {
                ThreadUtils.checkInterrupted(e);
                handleException(e);
            }
            break;
        }
    }

    private void publishEvent(TreeCacheEvent.Type type)
    {
        publishEvent(new TreeCacheEvent(type, null));
    }

    private void publishEvent(TreeCacheEvent.Type type, String path)
    {
        publishEvent(new TreeCacheEvent(type, new ChildData(path, null, null)));
    }

    private void publishEvent(TreeCacheEvent.Type type, ChildData data)
    {
        publishEvent(new TreeCacheEvent(type, data));
    }

    private void publishEvent(final TreeCacheEvent event)
    {
        if ( treeState.get() != TreeState.CLOSED )
        {
            LOG.debug("publishEvent: {}", event);
            executorService.submit(new Runnable()
            {
                @Override
                public void run()
                {
                    {
                        try
                        {
                            callListeners(event);
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            handleException(e);
                        }
                    }
                }
            });
        }
    }
}
