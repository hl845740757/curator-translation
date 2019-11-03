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
package org.apache.curator.framework.recipes.shared;

import org.apache.curator.framework.state.ConnectionStateListener;

/**
 * {@link SharedCountReader}的监听器
 *
 * Listener for changes to a shared count
 */
public interface SharedCountListener extends ConnectionStateListener
{
    /**
     * 当共享计数器的值发生改变时会回调该方法。
     *
     * Called when the shared value has changed
     *
     * @param sharedCount the shared count instance
     *                    共享计数器对象
     * @param newCount the new count
     *                 共享计数产生事件时的值(其实可能是个旧值)
     * @throws Exception errors
     */
    public void         countHasChanged(SharedCountReader sharedCount, int newCount) throws Exception;
}
