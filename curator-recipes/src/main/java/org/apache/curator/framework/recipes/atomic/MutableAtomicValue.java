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
 * 可变的原子值对象
 * @param <T> 数据的表示对象
 */
class MutableAtomicValue<T> implements AtomicValue<T>
{
    /** 原子对象操作前的值 */
    T preValue;
    /** 原子对象操作后的值 */
    T postValue;
    /** 操作是否成功 */
    boolean succeeded = false;

    /** 更新操作的过程信息 */
    AtomicStats stats = new AtomicStats();

    MutableAtomicValue(T preValue, T postValue)
    {
        this(preValue, postValue, false);
    }

    MutableAtomicValue(T preValue, T postValue, boolean succeeded)
    {
        this.preValue = preValue;
        this.postValue = postValue;
        this.succeeded = succeeded;
    }

    @Override
    public T preValue()
    {
        return preValue;
    }

    @Override
    public T postValue()
    {
        return postValue;
    }

    @Override
    public boolean succeeded()
    {
        return succeeded;
    }

    @Override
    public AtomicStats getStats()
    {
        return stats;
    }
}
