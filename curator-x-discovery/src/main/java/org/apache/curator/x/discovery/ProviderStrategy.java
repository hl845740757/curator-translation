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
package org.apache.curator.x.discovery;

import org.apache.curator.x.discovery.details.InstanceProvider;

/**
 * A strategy for picking one from a set of instances
 */
public interface ProviderStrategy<T>
{
    /**
     * 给定一组实例，返回其中的一个实例。
     *
     * Given a source of instances, return one of them for a single use.
     *
     * @param instanceProvider the instance provider
     *                         使用provider可以减少不必要的消耗，可以延迟创建动作，因为策略不一定需要获取所有的实例。
     *                         (这是函数式编程的好处)
     * @return the instance to use
     *                         用于提供服务的实例
     * @throws Exception any errors
     */
    public ServiceInstance<T>       getInstance(InstanceProvider<T> instanceProvider) throws Exception;
}
