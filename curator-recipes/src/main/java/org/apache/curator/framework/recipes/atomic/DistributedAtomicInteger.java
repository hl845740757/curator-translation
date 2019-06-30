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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

/**
 * 分布式原子变量(Integer)。一个尝试原子方式更新的计数器。
 * <p> 它首先会尝试使用乐观锁的方式更新。如果乐观锁操作失败，将会使用可选的互斥锁{@link InterProcessMutex}，
 * 对于乐观锁和互斥锁，都会使用重试策略{@link RetryPolicy}再次尝试更新。</p>
 *
 * <p>各种更新方法都会返回一个{@link AtomicValue}对象。你必须始终检查{@link AtomicValue#succeeded()}确定操作是否成功。
 * 除{@link #get()}方法外，所有方法都不保证成功。</p>
 *
 * {@link DistributedAtomicInteger}只是{@link DistributedAtomicValue}的一个特化类，最重要的逻辑是负责解析数据。
 * 核心的逻辑其实在{@link DistributedAtomicValue}中。
 *
 * 该类其实就干了两件事：
 * 1. {@link MakeValue} 根据旧值计算新值。
 * 2. {@link AtomicInteger} 提供字节数据与long之间的转换。
 *
 * <p>A counter that attempts atomic increments. It first tries uses optimistic locking. If that fails,
 * an optional {@link InterProcessMutex} is taken. For both optimistic and mutex, a retry policy is used to
 * retry the increment.</p>
 *
 * <p>The various increment methods return an {@link AtomicValue} object. You must <b>always</b> check
 * {@link AtomicValue#succeeded()}. None of the methods (other than get()) are guaranteed to succeed.</p>
 */
public class DistributedAtomicInteger implements DistributedAtomicNumber<Integer>
{
    private final DistributedAtomicValue        value;

    /**
     * 以仅乐观锁模式创建对象。即：不会升级到互斥锁。
     * （使用该构造方法创建的该对象只使用乐观锁模式更新数据，不会升级到互斥锁）
     *
     * Creates in optimistic mode only - i.e. the promotion to a mutex is not done
     *
     * @param client the client
     * @param counterPath path to hold the value
     * @param retryPolicy the retry policy to use
     */
    public DistributedAtomicInteger(CuratorFramework client, String counterPath, RetryPolicy retryPolicy)
    {
        this(client, counterPath, retryPolicy, null);
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
     * @param client the client
     * @param counterPath path to hold the value
     * @param retryPolicy the retry policy to use
     * @param promotedToLock the arguments for the mutex promotion
     */
    public DistributedAtomicInteger(CuratorFramework client, String counterPath, RetryPolicy retryPolicy, PromotedToLock promotedToLock)
    {
        value = new DistributedAtomicValue(client, counterPath, retryPolicy, promotedToLock);
    }

    @Override
    public AtomicValue<Integer>     get() throws Exception
    {
        return new AtomicInteger(value.get());
    }

    @Override
    public void forceSet(Integer newValue) throws Exception
    {
        value.forceSet(valueToBytes(newValue));
    }

    @Override
    public AtomicValue<Integer> compareAndSet(Integer expectedValue, Integer newValue) throws Exception
    {
        return new AtomicInteger(value.compareAndSet(valueToBytes(expectedValue), valueToBytes(newValue)));
    }

    @Override
    public AtomicValue<Integer>   trySet(Integer newValue) throws Exception
    {
        return new AtomicInteger(value.trySet(valueToBytes(newValue)));
    }

    @Override
    public boolean initialize(Integer initialize) throws Exception
    {
        return value.initialize(valueToBytes(initialize));
    }

    /**
     * @see DistributedAtomicNumber#increment()
     *
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<Integer>    increment() throws Exception
    {
        return worker(1);
    }

    /**
     * @see DistributedAtomicNumber#decrement()
     *
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<Integer>    decrement() throws Exception
    {
        return worker(-1);
    }

    /**
     * @see DistributedAtomicNumber#add(Object)
     *
     * Add delta to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to add
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<Integer>    add(Integer delta) throws Exception
    {
        return worker(delta);
    }

    /**
     * @see DistributedAtomicNumber#subtract(Object)
     *
     * Subtract delta from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to subtract
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    @Override
    public AtomicValue<Integer> subtract(Integer delta) throws Exception
    {
        return worker(-1 * delta);
    }

    /**
     * 将int值转换为字节数组。
     * 更常见的可能是{@link com.google.common.primitives.Ints#toByteArray(int)}
     * @param newValue int
     * @return bytes
     */
    @VisibleForTesting
    byte[] valueToBytes(Integer newValue)
    {
        Preconditions.checkNotNull(newValue, "newValue cannot be null");

        byte[]                      newData = new byte[4];
        ByteBuffer wrapper = ByteBuffer.wrap(newData);
        wrapper.putInt(newValue);
        return newData;
    }

    /**
     * 将字节数组转换为int值;
     * 更常见的可能是{@link com.google.common.primitives.Ints#fromByteArray(byte[])}
     * @param data bytes
     * @return int
     */
    @VisibleForTesting
    int bytesToValue(byte[] data)
    {
        if ( (data == null) || (data.length == 0) )
        {
            return 0;
        }
        ByteBuffer wrapper = ByteBuffer.wrap(data);
        try
        {
            return wrapper.getInt();
        }
        catch ( BufferUnderflowException e )
        {
            throw value.createCorruptionException(data);
        }
        catch ( BufferOverflowException e )
        {
            throw value.createCorruptionException(data);
        }
    }

    /**
     * 这是{@link DistributedAtomicInteger}真正有逻辑实现的部分
     * @param addAmount 增量（差量）
     * @return 操作结果
     * @throws Exception zookeeper errors
     */
    private AtomicValue<Integer>   worker(final Integer addAmount) throws Exception
    {
        Preconditions.checkNotNull(addAmount, "addAmount cannot be null");
        // 根据旧值计算新值的函数（int值加减操作）
        MakeValue               makeValue = new MakeValue()
        {
            @Override
            public byte[] makeFrom(byte[] previous)
            {
                // 根据前一个值，计算最新的值，加上指定增量得到结果
                int        previousValue = (previous != null) ? bytesToValue(previous) : 0;
                int        newValue = previousValue + addAmount;
                return valueToBytes(newValue);
            }
        };

        // 尝试将节点设置为指定数据
        AtomicValue<byte[]>     result = value.trySet(makeValue);
        return new AtomicInteger(result);
    }

    /**
     * 原子对象的int类型表示
     */
    private class AtomicInteger implements AtomicValue<Integer>
    {
        private AtomicValue<byte[]> bytes;

        private AtomicInteger(AtomicValue<byte[]> bytes)
        {
            this.bytes = bytes;
        }

        @Override
        public boolean succeeded()
        {
            return bytes.succeeded();
        }

        @Override
        public Integer preValue()
        {
            return bytesToValue(bytes.preValue());
        }

        @Override
        public Integer postValue()
        {
            return bytesToValue(bytes.postValue());
        }

        @Override
        public AtomicStats getStats()
        {
            return bytes.getStats();
        }
    }
}
