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
 * 从（分布式）原子对象返回一个的值的抽象；
 *
 * Abstracts a value returned from one of the Atomics
 */
public interface AtomicValue<T>
{
    /**
     * 本次更新操作是否成功；
     * 该值必须检查；如果方法返回ture，表示对应的操作成功；如果返回false，表示对应的操作失败，并且原子对象没有被更新；
     *
     * <b>MUST be checked.</b> Returns true if the operation succeeded. If false is returned,
     * the operation failed and the atomic was not updated.
     *
     * @return true/false
     */
    public boolean      succeeded();

    /**
     * 当操作成功时，返回该原子对象的前一个值。
     *
     * Returns the value of the counter prior to the operation
     *
     * @return pre-operation value
     */
    public T            preValue();

    /**
     * 当操作成功时，返回操作之后的最新值；
     *
     * Returns the value of the counter after to the operation
     *
     * @return post-operation value
     */
    public T            postValue();

    /**
     * Returns debugging stats about the operation
     *
     * @return stats
     */
    public AtomicStats  getStats();
}
