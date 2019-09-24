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
package org.apache.curator.x.discovery.strategies;

import org.apache.curator.x.discovery.ProviderStrategy;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.details.InstanceProvider;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 粘性策略 - 如果一个服务可用，那么总是使用它，一旦它不可用，再挑选新的。
 *
 * 此策略使用主策略来选择初始实例（委托给另一个策略实现真正的选择）。一旦选择了某个策略，该实例将始终返回。
 * 但是，如果当前选定的实例不在列表中，则使用主策略来选择新实例。
 *
 * This strategy uses a master strategy to pick the initial instance. Once picked,
 * that instance is always returned. If, however, the currently selected instance
 * is no longer in the list, the master strategy is used to pick a new instance.
 */
public class StickyStrategy<T> implements ProviderStrategy<T>
{
    private final ProviderStrategy<T>                   masterStrategy;
    private final AtomicReference<ServiceInstance<T>>   ourInstance = new AtomicReference<ServiceInstance<T>>(null);
    private final AtomicInteger                         instanceNumber = new AtomicInteger(-1);

    /**
     * @param masterStrategy the strategy to use for picking the sticky instance
     */
    public StickyStrategy(ProviderStrategy<T> masterStrategy)
    {
        this.masterStrategy = masterStrategy;
    }

    // 这块代码实现，我有点不是很赞同 缺少原始的注释，不是特别明白作者的思路。
    @Override
    public ServiceInstance<T> getInstance(InstanceProvider<T> instanceProvider) throws Exception
    {
        final List<ServiceInstance<T>>    instances = instanceProvider.getInstances();

        // 判断上次提供服务的实例是否还存在
        {
            ServiceInstance<T>                localOurInstance = ourInstance.get();
            if ( !instances.contains(localOurInstance) )
            {
                // 让旧数据过期
                ourInstance.compareAndSet(localOurInstance, null);
            }
            // 为什么不在else分支应该直接返回 localOurInstance ？？？？
        }

        // 这里也没有存为临时变量，即使不等于null，最后返回的也可能为null
        if ( ourInstance.get() == null )
        {
            // 注意：这里必须保证一致性，因此不能传入 instanceProvider， 因为每次调用provider的方法的结果可能是不一样的。
            // 因此返回了instances对象
            ServiceInstance<T> instance = masterStrategy.getInstance
            (
                new InstanceProvider<T>()
                {
                    @Override
                    public List<ServiceInstance<T>> getInstances() throws Exception
                    {
                       return instances;
                    }
                }
            );
            if ( ourInstance.compareAndSet(null, instance) )
            {
                instanceNumber.incrementAndGet();
                // 这里为什么不返回instance对象？
                // 没有注释，很难知道别人写代码的时候想的什么啊。。。 我猜测:尽量使用最新的对象。
                // 多线程、分布式下，返回任意一个看见的对象其实都是合理的，因为每一个看见的对象，可能都是旧对象。
            }
        }
        // 这里返回的值其实是不可推测的。
        return ourInstance.get();
    }

    /**
     * Each time a new instance is picked, an internal counter is incremented. This way you
     * can track when/if the instance changes. The instance can change when the selected instance
     * is not in the current list of instances returned by the instance provider
     *
     * @return instance number
     */
    public int getInstanceNumber()
    {
        return instanceNumber.get();
    }
}
