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
package org.apache.curator.framework.listen;

import com.google.common.util.concurrent.MoreExecutors;

import java.util.concurrent.Executor;

/**
 * 监听器及其执行器的通用持有者POJO
 *
 * Generic holder POJO for a listener and its executor
 * @param <T> the listener type
 */
public class ListenerEntry<T>
{
    /** 持有的listener */
    public final T        listener;
    /**
     * 该listener的运行环境 - 默认为{@link MoreExecutors#directExecutor()}，即在事件派发线程立即执行。
     */
    public final Executor executor;

    public ListenerEntry(T listener, Executor executor)
    {
        this.listener = listener;
        this.executor = executor;
    }
}
