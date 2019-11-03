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
 * 代码结构同{@link CachedAtomicInteger}；
 *
 * (预分配)支持缓存的原子变量；
 * 它使用一个{@link DistributedAtomicNumber}，并且按块的方式分配(申请)值以获得更好的性能。
 *
 * Uses an {@link DistributedAtomicNumber} and allocates values in chunks for better performance
 */
public class CachedAtomicLong
{
    /** 使用的原子对象，节点数据为long类型 */
    private final DistributedAtomicLong number;
    /** 预分配大小(块大小、段大小) ，表面上是个long，构造方法却使用int..*/
    private final long                  cacheFactor;
    /**
     * 当前缓存信息，它是一个操作结果，其中包含操作前的值和操作后的值。
     * 可用区间为：
     * min: {@link AtomicValue#preValue()} exclusive 操作前的值，表示他人已使用，因此我不能使用；
     * max: {@link AtomicValue#postValue()} inclusive 操作后的值，表示我申请的值，我可以使用；
     */
    private AtomicValue<Long>          currentValue = null;
    /**
     * 当前段的分配索引；
     * cacheFactor表面上是long，实际上也是int，因此currentIndex也使用int
     */
    private int                        currentIndex = 0;

    /**
     * @param number the number to use
     * @param cacheFactor the number of values to allocate at a time
     */
    public CachedAtomicLong(DistributedAtomicLong number, int cacheFactor)
    {
        this.number = number;
        this.cacheFactor = cacheFactor;
    }

    /**
     * @see CachedAtomicInteger#next()
     * 返回下一个值（当前值+1）。如果需要一个新的数字段，将会自动从{@link #number}中申请。
     *
     * Returns the next value (incrementing by 1). If a new chunk of numbers is needed, it is
     * requested from the number
     *
     * @return next increment
     * @throws Exception errors
     */
    public AtomicValue<Long>       next() throws Exception
    {
        MutableAtomicValue<Long> result = new MutableAtomicValue<Long>(0L, 0L);

        if ( currentValue == null )
        {
            // 当前段还未初始化，需要（重新）初始化
            currentValue = number.add(cacheFactor);
            if ( !currentValue.succeeded() )
            {
                // 操作失败，清除缓存，返回失败结果
                currentValue = null;
                result.succeeded = false;
                return result;
            }
            // 原子对象更新成功，重置索引
            currentIndex = 0;
        }

        // 当前在缓存中分配值，一定成功
        result.succeeded = true;
        result.preValue = currentValue.preValue() + currentIndex;
        result.postValue = result.preValue + 1;

        if ( ++currentIndex >= cacheFactor )
        {
            // 当前段数值使用完了，需要重新分配
            currentValue = null;
        }

        return result;
    }
}
