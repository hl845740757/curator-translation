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
package org.apache.curator.framework.recipes.leader;

/**
 * {@link LeaderLatch}状态改变监听器。
 * 当{@link LeaderLatch}的状态发生改变时，监听器将会被异步的通知。
 * 注意：因为您仅仅正处于其中一个方法调用的中间，并不一定意味着hasLeadership()是对应的true/false值。 -- 因为事件是异步的。
 * (在成为leader、失去leader事件产生之后)在调用这些方法之前，状态可能会在后台被更改。
 * 约定是，如果发生这种情况，您应该很快看到对另一个方法的另一个调用。
 *
 * A LeaderLatchListener can be used to be notified asynchronously about when the state of the LeaderLatch has changed.
 *
 * Note that just because you are in the middle of one of these method calls, it does not necessarily mean that
 * hasLeadership() is the corresponding true/false value.  It is possible for the state to change behind the scenes
 * before these methods get called.  The contract is that if that happens, you should see another call to the other
 * method pretty quickly.
 */
public interface LeaderLatchListener
{
  /**
   * 当成功成为leader时该方法将会被调用。
   * <p>
   * 注意：由于方法是异步调用的，因此当方法被调用时,{@link LeaderLatch#hasLeadership()}可能变为了false。
   * 如果发生了这种情况，你可以等待{@link #notLeader()}也被调用。
   *
   * This is called when the LeaderLatch's state goes from hasLeadership = false to hasLeadership = true.
   *
   * Note that it is possible that by the time this method call happens, hasLeadership has fallen back to false.  If
   * this occurs, you can expect {@link #notLeader()} to also be called.
   */
  public void isLeader();

  /**
   * 当失去leader领导权时该方法将会被调用。
   * <p>
   * 注意：由于该方法是异步调用的，因此当该方法执行时，{@link LeaderLatch#hasLeadership()}可能变为了true。
   * 如果发生了这种情况，你可以等待{@link #isLeader()}也被调用。
   *
   * This is called when the LeaderLatch's state goes from hasLeadership = true to hasLeadership = false.
   *
   * Note that it is possible that by the time this method call happens, hasLeadership has become true.  If
   * this occurs, you can expect {@link #isLeader()} to also be called.
   */
  public void notLeader();
}
