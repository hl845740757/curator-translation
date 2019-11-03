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
package org.apache.curator.framework.recipes.atomic;

import org.apache.curator.RetryLoop;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.PathUtils;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import java.util.Arrays;

/**
 * <p>一个尝试原子方式更新的分布式下的值(数据)，int，long，string都只是bytes一种表现形式而已。
 * 它首先会尝试使用乐观锁的方式更新。如果乐观锁操作失败，将会使用可选的互斥锁{@link InterProcessMutex}，
 * 对于乐观锁和互斥锁，都会使用重试策略{@link RetryPolicy}再次尝试更新。</p>
 *
 * <p>各种更新方法都会返回一个{@link AtomicValue}对象。你必须始终检查{@link AtomicValue#succeeded()}确定操作是否成功。
 * 除{@link #get()}方法外，所有方法都不保证成功。</p>
 *
 * 安全使用该类的前提：
 * 1. 所有客户端对该节点的使用方式必须是相同的。
 * 2. 节点一旦创建，不可以删除。
 *
 * <p>A distributed value that attempts atomic sets. It first tries uses optimistic locking. If that fails,
 * an optional {@link InterProcessMutex} is taken. For both optimistic and mutex, a retry policy is used to
 * retry the increment.</p>
 *
 * <p>The various methods return an {@link AtomicValue} object. You must <b>always</b> check
 * {@link AtomicValue#succeeded()}. None of the methods (other than get()) are guaranteed to succeed.</p>
 */
public class DistributedAtomicValue
{
    /** curator客户端 */
    private final CuratorFramework  client;
    /** 数据所在的节点路径 */
    private final String            path;
    /** 重试策略 */
    private final RetryPolicy       retryPolicy;

    /** 乐观锁升级到互斥锁的信息 */
    private final PromotedToLock    promotedToLock;

    /** 互斥锁 */
    private final InterProcessMutex mutex;

    /**
     * 以仅乐观锁模式创建对象。即：不会升级到互斥锁。
     * （使用该构造方法创建的该对象只使用乐观锁模式更新数据，不会升级到互斥锁）
     *
     * Creates in optimistic mode only - i.e. the promotion to a mutex is not done
     *
     * @param client the client curator客户端
     * @param path path to hold the value 数据所在的路径
     * @param retryPolicy the retry policy to use 重试策略
     */
    public DistributedAtomicValue(CuratorFramework client, String path, RetryPolicy retryPolicy)
    {
        this(client, path, retryPolicy, null);
    }

    /**
     * 以锁膨胀模式创建对象（乐观锁升级到互斥锁）。
     * 进行更新操作时，会首先使用乐观锁和给定的重试策略进行第一轮操作尝试。
     * 如果(乐观锁)更新操作失败，将会使用互斥锁{@link InterProcessMutex}和给定的重试策略进行第二轮操作尝试。
     *
     * Creates in mutex promotion mode. The optimistic lock will be tried first using
     * the given retry policy. If the increment does not succeed, a {@link InterProcessMutex} will be tried
     * with its own retry policy
     *
     * @param client the client curator客户端
     * @param path path to hold the value 数据所在的路径
     * @param retryPolicy the retry policy to use 重试策略
     * @param promotedToLock the arguments for the mutex promotion 由乐观锁升级到互斥锁的信息
     */
    public DistributedAtomicValue(CuratorFramework client, String path, RetryPolicy retryPolicy, PromotedToLock promotedToLock)
    {
        this.client = client;
        this.path = PathUtils.validatePath(path);
        this.retryPolicy = retryPolicy;
        this.promotedToLock = promotedToLock;
        mutex = (promotedToLock != null) ? new InterProcessMutex(client, promotedToLock.getPath()) : null;
    }

    /**
     * 获取计数器的当前值。
     * 注意：如果该节点还未被赋值过(节点不存在)，则返回0；
     *
     * Returns the current value of the counter. NOTE: if the value has never been set,
     * <code>0</code> is returned.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<byte[]>     get() throws Exception
    {
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);
        getCurrentValue(result, new Stat());
        result.postValue = result.preValue;
        result.succeeded = true;
        return result;
    }

    /**
     * 强制设置节点为指定值，不提供任何原子性保证。
     *
     * Forcibly sets the value without any guarantees of atomicity.
     *
     * @param newValue the new value
     * @throws Exception ZooKeeper errors
     */
    public void forceSet(byte[] newValue) throws Exception
    {
        // 首先假设了节点存在，直接进行一次赋值操作。
        // 为什么先尝试setData？因为节点不存在的情况的可能性是最小的，多数情况下节点是存在的。可能性最高的放在第一。
        // 也不需要浪费精力先检查后执行，因为先检查后执行是没有意义的，还浪费一次通信时间。
        try
        {
            client.setData().forPath(path, newValue);
        }
        catch ( KeeperException.NoNodeException dummy )
        {
            // 检测到节点不存在，需要创建节点，创建节点是一个并发操作(不同的客户端)，因此可能失败。
            // 这里无法确定其父节点是否存在，因此必须创建必要的父节点。
            try
            {
                client.create().creatingParentContainersIfNeeded().forPath(path, newValue);
            }
            catch ( KeeperException.NodeExistsException dummy2 )
            {
                // 创建节点失败，说明节点现在存在，由于节点不会被删除，因此可以进行赋值操作。
                // 注意前提条件：节点不可以删除。如果节点会删除，那么这里也是可能失败的！
                client.setData().forPath(path, newValue);
            }
        }
    }

    /**
     * 当且仅当原子对象的当前值等于给定的期望值的时候，进行一次更新操作；
     * 记得检查{@link AtomicValue#succeeded()}以判断操作是否成功。
     *
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     * Remember to always check {@link AtomicValue#succeeded()}.
     *
     *
     * @param expectedValue the expected value
     * @param newValue the new value
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<byte[]> compareAndSet(byte[] expectedValue, byte[] newValue) throws Exception
    {
        // 存储节点的当前状态
        Stat                        stat = new Stat();
        // 存储操作信息
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);
        // 是否需要创建节点
        boolean                     createIt = getCurrentValue(result, stat);
        if ( !createIt && Arrays.equals(expectedValue, result.preValue) )
        {
            // 节点已存在，且节点当前数据与期望的数据一致，尝试进行原子的更新
            try
            {
                // 本质上是基于版本号的CAS更新操作
                client.setData().withVersion(stat.getVersion()).forPath(path, newValue);
                result.succeeded = true;
                result.postValue = newValue;
            }
            catch ( KeeperException.BadVersionException dummy )
            {
                // 版本不匹配，CAS失败
                result.succeeded = false;
            }
            catch ( KeeperException.NoNodeException dummy )
            {
                // 节点不存在。
                // 如果要安全的使用该类，那么节点不应该删除。
                result.succeeded = false;
            }
        }
        else
        {
            // 节点不存在，或节点当前数据不等于期望值，无法使用CAS操作进行更新，操作失败。
            // 设计上没有允许null呢
            result.succeeded = false;
        }
        return result;
    }

    /**
     * 尝试原子的方式将节点数据设置为指定值。
     * 记住始终要检查{@link AtomicValue#succeeded()}判断操作是否成功。
     *
     * Attempt to atomically set the value to the given value. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param newValue the value to set 目标值
     * @return value info 操作结果信息，包含值的最新信息
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<byte[]>   trySet(final byte[] newValue) throws Exception
    {
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);

        // 计算新值的策略，由于新值与旧值无关，因此始终返回指定值
        MakeValue                   makeValue = new MakeValue()
        {
            @Override
            public byte[] makeFrom(byte[] previous)
            {
                // trySet 新值与旧值无关，始终返回指定值
                return newValue;
            }
        };
        // 首先尝试使用乐观锁进行更新
        tryOptimistic(result, makeValue);
        if ( !result.succeeded() && (mutex != null) )
        {
            // 乐观锁更新失败，且有互斥锁，尝试使用互斥锁更新
            tryWithMutex(result, makeValue);
        }

        return result;
    }

    /**
     * 当且仅当该节点的数据在数据库中为null时（节点不存在时），将其初始化为指定值。
     *
     * Atomic values are initially set to the equivalent of <code>NULL</code> in a database.
     * Use this method to initialize the value. The value will be set if and only iff the node does not exist.
     *
     * @param value the initial value to set 指定的初始值
     * @return true if the value was set, false if the node already existed
     *          如果返回true表示初始化成功，返回false表示节点已经存在了。
     * @throws Exception ZooKeeper errors
     */
    public boolean initialize(byte[] value) throws Exception
    {
        try
        {
            // 进行创建节点操作
            client.create().creatingParentContainersIfNeeded().forPath(path, value);
        }
        catch ( KeeperException.NodeExistsException ignore )
        {
            // ignore 节点已存在，被其它客户端创建了，忽略
            return false;
        }
        return true;
    }

    /**
     * 尝试进行一次设置操作
     * @param makeValue 根据旧值计算新值的函数
     * @return 操作结果
     * @throws Exception zookeeper errors
     */
    AtomicValue<byte[]>   trySet(MakeValue makeValue) throws Exception
    {
        // 存储结果信息
        MutableAtomicValue<byte[]>  result = new MutableAtomicValue<byte[]>(null, null, false);
        // 首先使用乐观锁进行一轮操作尝试
        tryOptimistic(result, makeValue);
        if ( !result.succeeded() && (mutex != null) )
        {
            // 乐观锁更新失败，且互斥锁存在时，尝试使用互斥锁进行更新
            tryWithMutex(result, makeValue);
        }

        return result;
    }

    /**
     * 创建替换用的错误信息。
     * @param bytes 数据
     * @return RuntimeException
     */
    RuntimeException createCorruptionException(byte[] bytes)
    {
        StringBuilder       str = new StringBuilder();
        str.append('[');
        boolean             first = true;
        for ( byte b : bytes )
        {
            if ( first )
            {
                first = false;
            }
            else
            {
                str.append(", ");
            }
            str.append("0x").append(Integer.toHexString((b & 0xff)));
        }
        str.append(']');
        return new RuntimeException(String.format("Corrupted data for node \"%s\": %s", path, str.toString()));
    }

    /**
     * 获取节点的当前值
     * @param result 用存储结果，将节点的当前值存储在{@link MutableAtomicValue#preValue}中
     * @param stat 存储节点状态
     * @return 是否需要创建节点
     * @throws Exception zookeeper errors
     */
    private boolean getCurrentValue(MutableAtomicValue<byte[]> result, Stat stat) throws Exception
    {
        // 默认假设节点存在，不需要创建节点
        boolean             createIt = false;
        try
        {
            // 尝试获取节点数据并存储在结果对象中
            result.preValue = client.getData().storingStatIn(stat).forPath(path);
        }
        catch ( KeeperException.NoNodeException e )
        {
            // 节点不存在
            result.preValue = null;
            createIt = true;
        }
        return createIt;
    }

    /**
     * 尝试使用互斥锁的方式更新数据
     * @param result 存储结果的容器
     * @param makeValue 根据节点当前数据，计算新数据的策略（函数）
     * @throws Exception zookeeper errors
     */
    private void tryWithMutex(MutableAtomicValue<byte[]> result, MakeValue makeValue) throws Exception
     {
         long            startMs = System.currentTimeMillis();
         // 重试次数
         int             retryCount = 0;

         // 在限定时间内尝试获取锁
         if ( mutex.acquire(promotedToLock.getMaxLockTime(), promotedToLock.getMaxLockTimeUnit()) )
         {
             // 成功获取到锁
             try
             {
                 // 操作是否结束（是否需要继续重试）
                 boolean         done = false;
                 while ( !done )
                 {
                     // 操作未完成，增加一次互斥锁下的尝试次数
                     result.stats.incrementPromotedTries();
                     // 尝试一次原子的更新操作
                     if ( tryOnce(result, makeValue) )
                     {
                         // 更新成功，操作完成
                         result.succeeded = true;
                         done = true;
                     }
                     else
                     {
                         // 原子操作失败，判断是否可以继续重试
                         // 在获得互斥锁的情况下，为什么会失败呢？
                         // 因为存在锁膨胀过程，我获得了互斥锁，只能互斥那些也处于互斥锁模式的客户端，并不能阻止其它客户端使用乐观锁更新数据！！！因此可能失败
                         if ( !promotedToLock.getRetryPolicy().allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                         {
                             // 不允许重试，操作结束（失败）
                             done = true;
                         }
                         // else 可以继续重试
                     }
                 }
             }
             finally
             {
                 // 释放获得的锁
                 mutex.release();
             }
         }

         result.stats.setPromotedTimeMs(System.currentTimeMillis() - startMs);
    }

    /**
     * 尝试以乐观锁的方式更新节点数据。
     * （和互斥锁的代码基本一致，只是统计的信息不一样）
     * {@link #tryWithMutex(MutableAtomicValue, MakeValue)}
     * @param result 存储结果信息的容器
     * @param makeValue 根据旧值计算新值的函数
     * @throws Exception zookeeper errors
     */
    private void tryOptimistic(MutableAtomicValue<byte[]> result, MakeValue makeValue) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        int             retryCount = 0;
        // 操作是否完成（是否需要继续重试）
        boolean         done = false;
        while ( !done )
        {
            // 增加乐观锁尝试次数
            result.stats.incrementOptimisticTries();
            // 尝试一次原子的更新操作
            if ( tryOnce(result, makeValue) )
            {
                // 更新成功，操作结束
                result.succeeded = true;
                done = true;
            }
            else
            {
                // 更新失败，判断是否可以继续充值
                if ( !retryPolicy.allowRetry(retryCount++, System.currentTimeMillis() - startMs, RetryLoop.getDefaultRetrySleeper()) )
                {
                    // 不允许重试，操作结束（失败）
                    done = true;
                }
                // else 可以继续重试
            }
        }

        result.stats.setOptimisticTimeMs(System.currentTimeMillis() - startMs);
    }

    /**
     * 尝试一次原子的更新操作
     * @param result 存储操作结果的容器
     * @param makeValue 通过旧值获得新值的函数
     * @return 是否更新成功，如果返回true表示操作成功，如果返回false表示操作失败
     * @throws Exception zookeeper errors
     */
    private boolean tryOnce(MutableAtomicValue<byte[]> result, MakeValue makeValue) throws Exception
    {
        Stat        stat = new Stat();
        // 先检查后执行
        boolean     createIt = getCurrentValue(result, stat);

        boolean     success = false;
        try
        {
            byte[]  newValue = makeValue.makeFrom(result.preValue);
            if ( createIt )
            {
                // 检查结果为节点不存在，尝试创建节点并初始化为指定值
                client.create().creatingParentContainersIfNeeded().forPath(path, newValue);
            }
            else
            {
                // 检查结果为节点存在，使用带版本号的更新操作，进行原子方式的更新
                client.setData().withVersion(stat.getVersion()).forPath(path, newValue);
            }
            // 操作成功，这里进行了一次保护性拷贝，确保安全性
            result.postValue = Arrays.copyOf(newValue, newValue.length);
            success = true;
        }
        catch ( KeeperException.NodeExistsException e )
        {
            // do Retry 检查结果为节点不存在，但是竞争创建节点失败
        }
        catch ( KeeperException.BadVersionException e )
        {
            // do Retry 检查结果为节点存在，但是在我尝试进行操作期间，其它客户端对该节点进行了操作。
        }
        catch ( KeeperException.NoNodeException e )
        {
            // do Retry 检查结果为节点存在，但是进行更新时节点被删除了。这个最好不要出现，节点尽量是不会被删除的！
        }

        return success;
    }
}
