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

/**
 * 分布式环境下的原子数字对象
 * @param <T> 数字的类型
 */
public interface DistributedAtomicNumber<T>
{
    /**
     * 返回计数器的当前值。
     * 注意：如果当前节点还未被赋值，则返回0
     * （在多线程和分布式环境下，就单某一个值而言，获取的时候是最新值，但立刻又可能成为旧值，因此先检查后执行的操作在多线程和分布式下要慎重）
     *
     * Returns the current value of the counter. NOTE: if the value has never been set,
     * <code>0</code> is returned.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T> get() throws Exception;

    /**
     * 当且仅当原子对象的当前值等于给定的期望值的时候，进行一次更新操作；
	 * 记得检查{@link AtomicValue#succeeded()}以判断操作是否成功。
     *
     * Atomically sets the value to the given updated value
     * if the current value {@code ==} the expected value.
     * Remember to always check {@link AtomicValue#succeeded()}.
     *
     * @param expectedValue the expected value
     * @param newValue      the new value for the counter
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T> compareAndSet(T expectedValue, T newValue) throws Exception;

    /**
	 * 尝试原子的将原子对象的值设置为指定值；
	 * 记得检查{@link AtomicValue#succeeded()}以判断操作是否成功。
	 *
     * Attempt to atomically set the value to the given value. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param newValue the value to set
     * @return value info 操作结果信息，包含了最新值，和更新前的值
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T> trySet(T newValue) throws Exception;

    /**
	 * 当前仅当原子对象在数据库中不存在（为NULL）时，将其初始化为指定值。
	 *
     * Atomic values are initially set to the equivalent of <code>NULL</code> in a database.
     * Use this method to initialize the value. The value will be set if and only iff the node does not exist.
     *
     * @param value the initial value to set
     * @return true if the value was set, false if the node already existed
	 * 			如果返回true表示初始化成功，返回false表示节点已经存在了。
     * @throws Exception ZooKeeper errors
     */
    public boolean initialize(T value) throws Exception;

    /**
	 * 在不保证任何原子性的情况下，强制将该计数器赋值为指定值。
	 *
     * Forcibly sets the value of the counter without any guarantees of atomicity.
     *
     * @param newValue the new value
     * @throws Exception ZooKeeper errors
     */
    public void forceSet(T newValue) throws Exception;

    /**
     * Add 1 to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T> increment() throws Exception;

    /**
     * Subtract 1 from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T> decrement() throws Exception;

    /**
     * Add delta to the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to add
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T> add(T delta) throws Exception;

    /**
     * Subtract delta from the current value and return the new value information. Remember to always
     * check {@link AtomicValue#succeeded()}.
     *
     * @param delta amount to subtract
     * @return value info
     * @throws Exception ZooKeeper errors
     */
    public AtomicValue<T> subtract(T delta) throws Exception;
}
