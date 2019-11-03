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
 * 控制TreeCache中的节点的处理。
 * 当尝试遍历一个节点的子节点时，仅当对该节点调用{@link #traverseChildren(String)}返回true时，才会查询该节点的子节点。
 * 当尝试缓存该节点的某个子节点时，仅当对子节点调用{@link #acceptChild(String)}返回true时，才会缓存该子节点。
 * （建议大家测试一下就知道了，文字稍微有点绕）
 * <p>
 *     Controls which nodes a TreeCache processes. When iterating
 *     over the children of a parent node, a given node's children are
 *     queried only if {@link #traverseChildren(String)} returns true.
 *     When caching the list of nodes for a parent node, a given node is
 *     stored only if {@link #acceptChild(String)} returns true.
 * </p>
 *
 * <p>
 *     E.g. Given:
 * <pre>
 * root
 *     n1-a
 *     n1-b
 *         n2-a
 *         n2-b
 *             n3-a
 *     n1-c
 *     n1-d
 * </pre>
 *     You could have a TreeCache only work with the nodes: n1-a, n1-b, n2-a, n2-b, n1-d
 *     by returning false from traverseChildren() for "/root/n1-b/n2-b" and returning
 *     false from acceptChild("/root/n1-c").
 * </p>
 */
public interface TreeCacheSelector
{
    /**
     * 确定是否需要便利该节点下的子节点（非叶子节点，包含根节点）。
     *（根节点就是第一个parent）
     *
     * Return true if children of this path should be cached.
     * i.e. if false is returned, this node is not queried to
     * determine if it has children or not
     *
     * @param fullPath full path of the ZNode
     * @return true/false
     */
    boolean traverseChildren(String fullPath);

    /**
     * 判断该节点是否需要缓存
     * （叶子节点和非叶子节点都会测试，不含根节点）
     * （任何一个节点都是根节点的子节点）
     *
     * Return true if this node should be returned from the cache
     *
     * @param fullPath full path of the ZNode 子节点的全路径
     * @return true/false
     */
    boolean acceptChild(String fullPath);
}
