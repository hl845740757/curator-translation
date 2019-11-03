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
package org.apache.curator.framework.recipes.locks;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * 标准的锁驱动
 */
public class StandardLockInternalsDriver implements LockInternalsDriver
{
    static private final Logger log = LoggerFactory.getLogger(StandardLockInternalsDriver.class);

    @Override
    public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
    {
        int             ourIndex = children.indexOf(sequenceNodeName);
        validateOurIndex(sequenceNodeName, ourIndex);
        // 是否获得锁，取决于在节点中的顺序 ---------- 最多有N个资源，前n个线程可以获得该锁
        boolean         getsTheLock = ourIndex < maxLeases;
        // 如果获得了锁，则不需要监听节点，否则监听ourIndex - maxLeases 下标的节点 - (不是很好表述)
        // 这样监听的好处？ 假设了ourIndex - maxLeases下标的节点删除，ourIndex所在的节点才可以获得锁，这样可以大幅度减少监听数/事件数。
        // 这样监听的缺点？ 当 maxLeases>1时(共享锁)，不能保证FIFO(公平)，由于获得锁的线程释放锁的时机不确定，因此可能后到的线程会先获得锁
        // 当然，对于锁来讲，一般不需要公平锁，没有必要的情况下，不要为公平付出代价。
        String          pathToWatch = getsTheLock ? null : children.get(ourIndex - maxLeases);

        return new PredicateResults(pathToWatch, getsTheLock);
    }

    @Override
    public String createsTheLock(CuratorFramework client, String path, byte[] lockNodeBytes) throws Exception
    {
        // 创建线程用于获取锁的节点，关键： EPHEMERAL_SEQUENTIAL， 节点为临时顺序节点，它提供了一定的顺序性保证，此外可防止死锁。
        // 当获得锁的客户端断开连接以后，临时节点最终会删除!这样其它客户端就能获得该锁。 redis是基于指定过期时间实现的，相对于zookeeper来讲，安全性差一点 ---- 获得锁后，在进行计算的过程中锁过期了就尴尬了。
        String ourPath;
        if ( lockNodeBytes != null )
        {
            // 指定了存储数据，使用指定数据创建节点
            ourPath = client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, lockNodeBytes);
        }
        else
        {
            // 没有额外要存储的数据
            ourPath = client.create().creatingParentContainersIfNeeded().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path);
        }
        return ourPath;
    }


    @Override
    public String fixForSorting(String str, String lockName)
    {
        return standardFixForSorting(str, lockName);
    }

    /**
     * 去除锁名字前缀。
     * lock-00000001 -> 00000001
     *
     * @param str 临时节点的全民
     * @param lockName 锁名字 -> 临时节点名字前缀
     * @return 去除锁名字前缀后的节点信息
     */
    public static String standardFixForSorting(String str, String lockName)
    {
        int index = str.lastIndexOf(lockName);
        if ( index >= 0 )
        {
            // 做了一步额外的校验，进行截断的时候，判断了是否溢出 lastIndexOf不应该出现这种情况吧？
            index += lockName.length();
            return index <= str.length() ? str.substring(index) : "";
        }
        return str;
    }

    /** 校验索引节点的合法性 */
    static void validateOurIndex(String sequenceNodeName, int ourIndex) throws KeeperException
    {
        // 创建了节点，结果又在某个时候节点消失了 - 被服务器删了(超时)，或其它客户端删了
        if ( ourIndex < 0 )
        {
            throw new KeeperException.NoNodeException("Sequential path not found: " + sequenceNodeName);
        }
    }
}
