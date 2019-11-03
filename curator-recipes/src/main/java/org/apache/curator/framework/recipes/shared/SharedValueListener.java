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
 * 共享数据监听器
 * <p>
 * Listener for changes to a shared value
 */
public interface SharedValueListener extends ConnectionStateListener
{
    /**
     * 当共享数据发生改变时将会回调该方法。
     * <p>
     * Called when the shared value has changed
     *
     * @param sharedValue the shared value instance
     *                    共享数据对应的实体
     * @param newValue    the new value 产生该事件时对应的数据。
     *                    注意：它是一个瞬时值，与{@link SharedValueReader#getValue()}可能不一致。
     *                    因为实体对象的数据可能会被更新。
     * @throws Exception errors
     */
    public void valueHasChanged(SharedValueReader sharedValue, byte[] newValue) throws Exception;
}
