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
 * (预分配)支持缓存的原子变量；
 * 它使用一个{@link DistributedAtomicNumber}，并且按块的方式分配(申请)值以获得更好的性能。
 * （其实自己实现更好，使用这个类其实挺麻烦的）
 *
 * Uses an {@link DistributedAtomicNumber} and allocates values in chunks for better performance
 */
public class CachedAtomicInteger
{
    /** 原子变量 */
    private final DistributedAtomicInteger  number;
    /** 预分配大小(块大小、段大小) */
    private final int                       cacheFactor;

    /**
     * 当前缓存信息，它是一个操作结果，其中包含操作前的值和操作后的值。
     * 可用区间为：
     * min: {@link AtomicValue#preValue()} exclusive 操作前的值，表示他人已使用，因此我不能使用；
     * max: {@link AtomicValue#postValue()} inclusive 操作后的值，表示我申请的值，我可以使用；
     */
    private AtomicValue<Integer>       currentValue = null;

    /** 当前段的分配索引 */
    private int                        currentIndex = 0;

    /**
     * @param number the number to use
     * @param cacheFactor the number of values to allocate at a time 缓存因子，缓存大小
     */
    public CachedAtomicInteger(DistributedAtomicInteger number, int cacheFactor)
    {
        this.number = number;
        this.cacheFactor = cacheFactor;
    }

    /**
     * 返回下一个值（当前值+1）。如果需要一个新的数字段，将会自动从{@link #number}中申请。
     *
     * Returns the next value (incrementing by 1). If a new chunk of numbers is needed, it is
     * requested from the number
     *
     * @return next increment
     * @throws Exception errors
     */
    public AtomicValue<Integer>       next() throws Exception
    {
        MutableAtomicValue<Integer> result = new MutableAtomicValue<Integer>(0, 0);

        if ( currentValue == null )
        {
            // 缓存值不存在，表示需要(重新)初始化，操作结果包含了分配之前的值和分配之后的值
            currentValue = number.add(cacheFactor);
            if ( !currentValue.succeeded() )
            {
                // 原子对象更新失败
                currentValue = null;
                result.succeeded = false;
                return result;
            }
            // 原子对象更新成功，重置索引
            currentIndex = 0;
        }

        // 从缓存内分配必定成功
        result.succeeded = true;
        result.preValue = currentValue.preValue() + currentIndex;
        result.postValue = result.preValue + 1;

        if ( ++currentIndex >= cacheFactor )
        {
            // 当前段数据已分配完毕，清除缓存，需要重新申请一段
            currentValue = null;
        }

        return result;
    }
}
