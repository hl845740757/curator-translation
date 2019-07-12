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

import java.util.concurrent.Executor;

/**
 * 抽象的可监听对象。
 * Listenable ：观察者模式中的主题对象。
 * Listener : 观察者模式中的观察者对象
 * Executor ：观察者运行的环境(线程)
 *
 * Abstracts a listenable object
 */
public interface Listenable<T>
{
    /**
     * 添加一个监听器，该监听器将在包含实例的线程中执行（事件通知线程）。
     *
     * 当你的代码支持并发调用的时候，那么使用该方法注册监听器即可。
     * @see #addListener(Object, Executor)
     *
     * Add the given listener. The listener will be executed in the containing
     * instance's thread.
     *
     * @param listener listener to add
     */
    public void     addListener(T listener);

    /**
     * 添加一个监听器，该监听器将在给定的executor中执行。
     * 如果要保证事件的顺序，那么{@link Executor}必须是单线程的！
     *
     * 直观的好处：当你的执行环境是一个 单线程的 executor的时候，可以直接提交到你所在的线程！从而消除不必要的同步。
     * （我越来越感受到Executor框架的好处了，不直接使用线程对象，而是使用Executor对象，这样会带来很多好处）
     * （不得不承认netty的E ventExecutor 和 EventLoop 设计的很好。）
     *
     * Add the given listener. The listener will be executed using the given
     * executor
     *
     * @param listener listener to add
     * @param executor executor to run listener in
     */
    public void     addListener(T listener, Executor executor);

    /**
     * 移除指定监听器。
     *
     * Remove the given listener
     *
     * @param listener listener to remove
     */
    public void     removeListener(T listener);
}
