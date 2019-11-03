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

package org.apache.curator.framework.recipes.shared;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.WatcherRemoveCuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 管理共享数据。观察同一路径的所有客户端都将具有最新的值（基于zookeeper的正常一致性保证）。
 * <p>
 * 看说明好像和{@link NodeCache}有点像，而且它的事件处理更加科学啊。{@link SharedValueListener#valueHasChanged(SharedValueReader, byte[])}
 * 是可以直接获得最新值的，而{@link NodeCacheListener#nodeChanged()}其实不能安全的获取产生事件时对应的数据。
 * 不过好像不能使用null作为数据。
 *
 * Manages a shared value. All clients watching the same path will have the up-to-date
 * value (considering ZK's normal consistency guarantees).
 */
public class SharedValue implements Closeable, SharedValueReader
{
	private static final int UNINITIALIZED_VERSION = -1;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ListenerContainer<SharedValueListener> listeners = new ListenerContainer<SharedValueListener>();
    private final WatcherRemoveCuratorFramework client;
    private final String path;
    /**
     * 初始值，当节点不存在时，用于初始化该节点的值 - 因为{@link SharedValue}不允许null值存在。
     */
    private final byte[] seedValue;
    /**
     * 缓存/共享数据的状态
     */
    private final AtomicReference<State> state = new AtomicReference<State>(State.LATENT);
    /**
     * 缓存/共享数据当前的<b>缓存值</b>
     */
    private final AtomicReference<VersionedValue<byte[]>> currentValue;
    private final CuratorWatcher watcher;

    private class SharedValueCuratorWatcher implements CuratorWatcher
    {
        @Override
        public void process(WatchedEvent event) throws Exception
        {
            if ( state.get() == State.STARTED && event.getType() != Watcher.Event.EventType.None )
            {
                // don't block event thread in possible retry
                // 后台拉取最新数据，是为了不阻塞主线程 - mainEventThread
                // 看这注释感觉和 NodeCache的作者不是同一个人
                readValueAndNotifyListenersInBackground();
            }
        }
    };

    private final ConnectionStateListener connectionStateListener = new ConnectionStateListener()
    {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState)
        {
            notifyListenerOfStateChanged(newState);
            if ( newState.isConnected() )
            {
                try
                {
                    readValueAndNotifyListenersInBackground();
                }
                catch ( Exception e )
                {
                    ThreadUtils.checkInterrupted(e);
                    log.error("Could not read value after reconnect", e);
                }
            }
        }
    };

    private enum State
    {
        /**
         * 初始状态，尚未启动
         */
        LATENT,
        /**
         * 运行状态
         */
        STARTED,
        /**
         * 已关闭状态
         */
        CLOSED
    }

    /**
     * @param client    the client
     * @param path      the shared path - i.e. where the shared value is stored
     *                  存储共享数据的路径
     * @param seedValue the initial value for the value if/f the path has not yet been created
     *                  当指定节点未创建时，用于初始化节点的数据
     */
    public SharedValue(CuratorFramework client, String path, byte[] seedValue)
    {
        this.client = client.newWatcherRemoveCuratorFramework();
        this.path = PathUtils.validatePath(path);
        this.seedValue = Arrays.copyOf(seedValue, seedValue.length);
        this.watcher = new SharedValueCuratorWatcher();
        currentValue = new AtomicReference<VersionedValue<byte[]>>(new VersionedValue<byte[]>(UNINITIALIZED_VERSION, Arrays.copyOf(seedValue, seedValue.length)));
    }

    @VisibleForTesting
    protected SharedValue(WatcherRemoveCuratorFramework client, String path, byte[] seedValue, CuratorWatcher watcher)
    {
        this.client = client;
        this.path = PathUtils.validatePath(path);
        this.seedValue = Arrays.copyOf(seedValue, seedValue.length);
        // inject watcher for testing
        this.watcher = watcher;
        currentValue = new AtomicReference<VersionedValue<byte[]>>(new VersionedValue<byte[]>(UNINITIALIZED_VERSION, Arrays.copyOf(seedValue, seedValue.length)));
    }

    @Override
    public byte[] getValue()
    {
        // 存为临时变量是保证下一行代码的两处引用是同一个对象 - 如果使用currentValue.get()就不一定是同一个对象
        VersionedValue<byte[]> localCopy = currentValue.get();
        // 拷贝数据，避免外部修改
        return Arrays.copyOf(localCopy.getValue(), localCopy.getValue().length);
    }

    @Override
    public VersionedValue<byte[]> getVersionedValue()
    {
        // 存为临时变量的原因同上，保证下一行的代码操作的是同一个对象 - 如果使用currentValue.get()就不一定是同一个对象
        VersionedValue<byte[]> localCopy = currentValue.get();
        return new VersionedValue<byte[]>(localCopy.getVersion(), Arrays.copyOf(localCopy.getValue(), localCopy.getValue().length));
    }

    /**
     * 更新共享数据
     *
     * Change the shared value value irrespective of its previous state
     *
     * @param newValue new value
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void setValue(byte[] newValue) throws Exception
    {
        Preconditions.checkState(state.get() == State.STARTED, "not started");

        Stat result = client.setData().forPath(path, newValue);
        // 更新远程成功，更新本地缓存
        updateValue(result.getVersion(), Arrays.copyOf(newValue, newValue.length));
    }

    /**
     * 该方法不再推荐使用：因为无法保证客户端是在看见最新值的情况下进行的更新。
     *
     * Changes the shared value only if its value has not changed since this client last
     * read it. If the value has changed, the value is not set and this client's view of the
     * value is updated. i.e. if the value is not successful you can get the updated value
     * by calling {@link #getValue()}.
     *
     * @deprecated use {@link #trySetValue(VersionedValue, byte[])} for stronger atomicity
     * guarantees. Even if this object's internal state is up-to-date, the caller has no way to
     * ensure that they've read the most recently seen value.
     *
     * @param newValue the new value to attempt
     * @return true if the change attempt was successful, false if not. If the change
     * was not successful, {@link #getValue()} will return the updated value
     * @throws Exception ZK errors, interruptions, etc.
     */
    @Deprecated
    public boolean trySetValue(byte[] newValue) throws Exception
    {
        return trySetValue(currentValue.get(), newValue);
    }

    /**
     * 尝试CAS更新共享数据的值，仅当共享值的值在newValue指定的版本之后没有更改时才更改该值，否则更新失败，并更新客户端的数据视图。
     * 例如：如果赋值失败，你可以调用{@link #getValue()}获取最新的值。
     *
     * Changes the shared value only if its value has not changed since the version specified by
     * newValue. If the value has changed, the value is not set and this client's view of the
     * value is updated. i.e. if the value is not successful you can get the updated value
     * by calling {@link #getValue()}.
     *
     * @param newValue the new value to attempt
     * @return true if the change attempt was successful, false if not. If the change
     * was not successful, {@link #getValue()} will return the updated value
     *      如果返回true，则表示更新成功，false则表示更新失败。如果更新失败，{@link #getValue()}将返回最新的值。
     * @throws Exception ZK errors, interruptions, etc.
     */
    public boolean trySetValue(VersionedValue<byte[]> previous, byte[] newValue) throws Exception
    {
        Preconditions.checkState(state.get() == State.STARTED, "not started");

        VersionedValue<byte[]> current = currentValue.get();
        if ( previous.getVersion() != current.getVersion() || !Arrays.equals(previous.getValue(), current.getValue()) )
        {
            // 与本地缓存不匹配，那么一定与远程不匹配。
            // 也证明客户端没有读取最新的值。
            // 这里并没有调用readValue读取最新的值，因为当前缓存值是比previous更新，用户应该使用最新的缓存进行尝试。
            return false;
        }

        // 与本地缓存最新值匹配，尝试更新远程节点数据
        try
        {
            Stat result = client.setData().withVersion(previous.getVersion()).forPath(path, newValue);
            // 更新远程成功，更新本地缓存
            updateValue(result.getVersion(), Arrays.copyOf(newValue, newValue.length));
            return true;
        }
        catch ( KeeperException.BadVersionException ignore )
        {
            // ignore
            // 版本不匹配，CAS更新失败，忽略异常
        }
        // 如果更新失败，则读取最新的值
        readValue();
        return false;
    }

    /**
     * 更新本地缓存 - 该方法可能被多个线程调用(多个客户端线程+1个mainEventThread后台线程)
     * @param version 要更新的数据对应的版本号
     * @param bytes 要更新的数据
     */
    private void updateValue(int version, byte[] bytes)
    {
        while (true)
        {
            VersionedValue<byte[]> current = currentValue.get();
            if (current.getVersion() >= version)
            {
                // A newer version was concurrently set.
                // 有人赋了一个更加新的值，那么不再更新
                return;
            }
            if ( currentValue.compareAndSet(current, new VersionedValue<byte[]>(version, bytes)) )
            {
                // 更新成功
                // Successfully set.
                return;
            }
            // Lost a race, retry.
            // 由于竞争导致更新失败，进行重试
        }
    }

    /**
     * 获取节点的监听器容器
     * Returns the listenable
     *
     * @return listenable
     */
    public ListenerContainer<SharedValueListener> getListenable()
    {
        return listeners;
    }

    /**
     * The shared value must be started before it can be used. Call {@link #close()} when you are
     * finished with the shared value
     *
     * @throws Exception ZK errors, interruptions, etc.
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "Cannot be started more than once");

        client.getConnectionStateListenable().addListener(connectionStateListener);
        try
        {
            // 尝试初始化节点，不允许节点数据为null
            client.create().creatingParentContainersIfNeeded().forPath(path, seedValue);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // ignore
            // 节点已经存在，可能被其它客户端创建了，那么什么也不需要做。
            // 因为客户端的初始化动作本质上是一样的，不论谁更新成功，都是等价的。
        }
        // 读取最新的节点数据收到本地
        // 这个似乎可以优化？
        // 1. 如果初始化节点成功，使用updateValue?
        // 2. 如果初始化失败，使用readValue？
        // 不行！因为create无法注册watcher！只有getData/checkExist可以注册watcher。而这里必须注册watcher监听节点数据变化。
        readValue();
    }

    @Override
    public void close() throws IOException
    {
        state.set(State.CLOSED);
        client.removeWatchers();
        client.getConnectionStateListenable().removeListener(connectionStateListener);
        listeners.clear();
    }

    /**
     * 拉取节点最新的数据到本地
     * @throws Exception errors
     */
    private void readValue() throws Exception
    {
        Stat localStat = new Stat();
        // 读取节点数据的时候，同时注册watcher
        byte[] bytes = client.getData().storingStatIn(localStat).usingWatcher(watcher).forPath(path);
        // 读取数据成功，更新到本地缓存
        updateValue(localStat.getVersion(), bytes);
    }

    private final BackgroundCallback upadateAndNotifyListenerCallback = new BackgroundCallback() {
        @Override
        public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
            if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                // 拉取数据成功 - 更新本地数据
                updateValue(event.getStat().getVersion(), event.getData());
                // 更新失败了还需要通知监听器吗？
                // 这里进行了通知，可能导致事件处理器看见的数据和事件本身的数据不一致。
                notifyListeners();
            }
        }
    };

    /**
     * 后台拉取数据并通知监听器（避免阻塞当前线程）。
     */
    private void readValueAndNotifyListenersInBackground() throws Exception
    {
        client.getData().usingWatcher(watcher).inBackground(upadateAndNotifyListenerCallback).forPath(path);
    }

    /**
     * 通知监听器们数据发生了改变
     */
    private void notifyListeners()
    {
        final byte[] localValue = getValue();
        listeners.forEach
            (
                new Function<SharedValueListener, Void>()
                {
                    @Override
                    public Void apply(SharedValueListener listener)
                    {
                        try
                        {
                            listener.valueHasChanged(SharedValue.this, localValue);
                        }
                        catch ( Exception e )
                        {
                            ThreadUtils.checkInterrupted(e);
                            log.error("From SharedValue listener", e);
                        }
                        return null;
                    }
                }
            );
    }

    /**
     * 通知监听器们连接状态发生了改变
     */
    private void notifyListenerOfStateChanged(final ConnectionState newState)
    {
        listeners.forEach
            (
                new Function<SharedValueListener, Void>()
                {
                    @Override
                    public Void apply(SharedValueListener listener)
                    {
                        listener.stateChanged(client, newState);
                        return null;
                    }
                }
            );
    }
}
