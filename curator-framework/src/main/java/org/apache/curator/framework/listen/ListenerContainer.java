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

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * listener的容器。实现listener的管理。
 * 算是“组合优于继承”一个很好的范例了，很多地方都使用到了该对象。
 *
 * Abstracts an object that has listeners
 *
 * @deprecated Prefer {@link MappingListenerManager} and
 * {@link StandardListenerManager}
 */
@Deprecated
public class ListenerContainer<T> implements Listenable<T>
{
    private final Logger                        log = LoggerFactory.getLogger(getClass());
    /**
     * 主要用于建立listener 到其 executor的映射
     * 由于使用的是{@link java.util.concurrent.ConcurrentMap}，在处理事件的过程中添加新的监听器，
     * 新的监听器可能立刻收到通知
     */
    private final Map<T, ListenerEntry<T>>      listeners = Maps.newConcurrentMap();

    /**
     * 添加一个监听器，并由[事件通知线程]处理事件。
     * 详细注释：{@link Listenable#addListener(Object) }
     *
     * @param listener listener to add
     */
    @Override
    public void addListener(T listener)
    {
        // sameThreadExecutor表示由调用execute方法的线程直接执行提交的任务。(也叫：CallerRuns 调用者执行)
        // 在这里就表示由抛出事件的线程执行事件处理。
        addListener(listener, MoreExecutors.directExecutor());
    }

    /**
     * 添加一个监听器，并由指定的executor处理事件。
     * 详细注释：{@link Listenable#addListener(Object, Executor) }
     *
     * @param listener listener to add.
     * @param executor executor to run listener in.
     */
    @Override
    public void addListener(T listener, Executor executor)
    {
        listeners.put(listener, new ListenerEntry<T>(listener, executor));
    }

    /**
     * 删除某个监听器
     * @param listener listener to remove
     */
    @Override
    public void removeListener(T listener)
    {
        if ( listener != null )
        {
            listeners.remove(listener);
        }
    }

    /**
     * 清空所有监听器
     * Remove all listeners
     */
    public void     clear()
    {
        listeners.clear();
    }

    /**
     * 获取当前监听器的数量
     *
     * Return the number of listeners
     *
     * @return number
     */
    public int      size()
    {
        return listeners.size();
    }

    /**
     * 对每一个监听器执行给定的函数。
     * 函数接收listener作为参数（因为listener具体类型是不确定的，要执行什么操作只有容器的拥有者才知道）。
     *
     * 为何要用{@link Function}，而不是Consumer?因为这是java1.6版本。
     * 此{@link Function}非彼Function
     *
     * Utility - apply the given function to each listener. The function receives
     * the listener as an argument.
     *
     * @param function function to call for each listener
     */
    public void     forEach(final Function<T, Void> function)
    {
        for ( final ListenerEntry<T> entry : listeners.values() )
        {
            // 将该操作提交到每一个executor，function最好是无状态的/线程安全的
            entry.executor.execute
            (
                new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try
                        {
                            function.apply(entry.listener);
                        }
                        catch ( Throwable e )
                        {
                            // 检查是否需要恢复中断
                            ThreadUtils.checkInterrupted(e);
                            log.error(String.format("Listener (%s) threw an exception", entry.listener), e);
                        }
                    }
                }
            );
        }
    }
}
