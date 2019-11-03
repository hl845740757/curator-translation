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
package org.apache.curator.framework.recipes.cache;

/**
 * 单节点缓存监听器。
 */
public interface NodeCacheListener
{
    /**
     * 当监听的节点状态出现改变时会调用该方法。
     *
     * Q:为什么该方法没有参数？
     * A:{@link NodeCache} 的数据由main-EventThread线程更新，并直接由main-EventThread通知监听者处理事件。
     * 更新数据的线程和处理事件的线程都是 mian-EventThread线程，并且是同步执行的。
     * 可以简单直接地直接使用{@link NodeCache#getCurrentData()}处理事件。因此不需要参数！
     *
     * 注意：可以将当前数据{@link NodeCache#getCurrentData()}发布到其它线程，但不能将{@link NodeCache}发布到其它线程。
     * {@link ChildData}是不可变对象，是线程安全的，而{@link NodeCache}是可变对象，多线程共享会出现线程安全问题。
     *
     * Called when a change has occurred
     */
    public void     nodeChanged() throws Exception;
}
