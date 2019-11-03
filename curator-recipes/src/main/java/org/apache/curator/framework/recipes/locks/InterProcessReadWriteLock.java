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

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 *
 * <p>
 *     这是一个跨越JVM的可重入读写锁（可重入分布式锁）。使用zookeeper持有锁。
 *     所有使用相同锁路径的JVM中的所有进程都将实现进程间的“临界区”。
 *     此外，这个互斥是“公平的”—每个用户将按请求的顺序获得互斥（从zk的角度看）
 * </p>
 *
 * <p>
 *     读写锁维护一对关联的锁，一个用于只读操作，一个用于写入。只要没有writer，多个reader就可以同时持有读锁，写锁是独占的。
 *     (和JDK的锁其实是类似的，读读共享，读写互斥，写读互斥，写写互斥)
 * </p>
 *
 * <h3>锁重入规则</h3>
 *     该锁读写都可以重入。在持有写锁的线程/进程释放写锁之前读锁无法重入。
 *     此外，一个writer(获得写锁的)可以继续获取读锁，反过来却不行。一个reader(获得读锁的线程)申请写锁将永远不会成功。
 *
 * <h3>锁降级</h3>
 *     可重入性还允许从写锁降级为读锁，方法是获取写锁，然后获取读锁，然后释放写锁。但是，无法从读锁升级到写锁。
 *     (为何不支持锁升级？你并不能让其他获得读锁的线程释放读锁)
 *
 * <p>
 *  猜测应该和JDK的读写锁相似。<br>
 *  锁节点仍然为父节点(容器节点)，所有申请锁的进程/线程在该节点下创建<b>临时有序节点{@link CreateMode#PERSISTENT_SEQUENTIAL}</b>，
 *  如果第一个节点为写请求，则该节点独占锁。否则，第一个写请求之前的所有读请求的节点共同获得该锁。
 * <p>
 *  真实实现呢？<br>
 *  就是上面描述的那样，只不过拆分为两个{@link InterProcessMutex}进行了分别管理。这样的话，每个{@link InterProcessMutex}只看自己前面有没有其它类型的节点。
 *  没有则可以获得锁。
 *
 * <p>
 *    A re-entrant read/write mutex that works across JVMs. Uses Zookeeper to hold the lock. All processes
 *    in all JVMs that use the same lock path will achieve an inter-process critical section. Further, this mutex is
 *    "fair" - each user will get the mutex in the order requested (from ZK's point of view).
 * </p>
 *
 * <p>
 *    A read write lock maintains a pair of associated locks, one for read-only operations and one
 *    for writing. The read lock may be held simultaneously by multiple reader processes, so long as
 *    there are no writers. The write lock is exclusive.
 * </p>
 *
 * <p>
 *    <b>Reentrancy</b><br>
 *    This lock allows both readers and writers to reacquire read or write locks in the style of a
 *    re-entrant lock. Non-re-entrant readers are not allowed until all write locks held by the
 *    writing thread/process have been released. Additionally, a writer can acquire the read lock, but not
 *    vice-versa. If a reader tries to acquire the write lock it will never succeed.<br><br>
 *
 *    <b>Lock downgrading</b><br>
 *    Re-entrancy also allows downgrading from the write lock to a read lock, by acquiring the write
 *    lock, then the read lock and then releasing the write lock. However, upgrading from a read
 *    lock to the write lock is not possible.
 * </p>
 */
public class InterProcessReadWriteLock
{
    /**
     * 读锁组件
     */
    private final InterProcessMutex readMutex;
    /**
     * 写锁组件
     */
    private final InterProcessMutex writeMutex;

    //  读写锁的前缀
    // must be the same length. LockInternals depends on it
    private static final String READ_LOCK_NAME  = "__READ__";
    private static final String WRITE_LOCK_NAME = "__WRIT__";

    private static class SortingLockInternalsDriver extends StandardLockInternalsDriver
    {

        @Override
        public final String fixForSorting(String str, String lockName)
        {
            // 删除节点的锁类型前缀  _READ_-00000001  -> 00000001
            str = super.fixForSorting(str, READ_LOCK_NAME);
            str = super.fixForSorting(str, WRITE_LOCK_NAME);
            return str;
        }
    }

    private static class InternalInterProcessMutex extends InterProcessMutex
    {
        /**
         *  锁的名字
         * {@link InterProcessReadWriteLock#READ_LOCK_NAME} 或
         * {@link InterProcessReadWriteLock#WRITE_LOCK_NAME}
         */
        private final String lockName;
        /** 该节点存储的数据 */
        private final byte[] lockData;

        InternalInterProcessMutex(CuratorFramework client, String path, String lockName, byte[] lockData, int maxLeases, LockInternalsDriver driver)
        {
            super(client, path, lockName, maxLeases, driver);
            this.lockName = lockName;
            this.lockData = lockData;
        }

        @Override
        public Collection<String> getParticipantNodes() throws Exception
        {
            // 获取所有的同类型节点 - 读锁或写锁
            Collection<String>  nodes = super.getParticipantNodes();
            Iterable<String>    filtered = Iterables.filter
            (
                nodes,
                new Predicate<String>()
                {
                    @Override
                    public boolean apply(String node)
                    {
                        return node.contains(lockName);
                    }
                }
            );
            return ImmutableList.copyOf(filtered);
        }

        @Override
        protected byte[] getLockNodeBytes()
        {
            return lockData;
        }
    }

  /**
    * @param client the client
    * @param basePath path to use for locking
    */
    public InterProcessReadWriteLock(CuratorFramework client, String basePath)
    {
        this(client, basePath, null);
    }

  /**
    * @param client the client
    * @param basePath path to use for locking
    * @param lockData the data to store in the lock nodes
    */
    public InterProcessReadWriteLock(CuratorFramework client, String basePath, byte[] lockData)
    {
        lockData = (lockData == null) ? null : Arrays.copyOf(lockData, lockData.length);

        writeMutex = new InternalInterProcessMutex
        (
            client,
            basePath,
            WRITE_LOCK_NAME,
            lockData,
            1,
            new SortingLockInternalsDriver()
            {
                @Override
                public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
                {
                    return super.getsTheLock(client, children, sequenceNodeName, maxLeases);
                }
            }
        );

        readMutex = new InternalInterProcessMutex
        (
            client,
            basePath,
            READ_LOCK_NAME,
            lockData,
            Integer.MAX_VALUE,
            new SortingLockInternalsDriver()
            {
                @Override
                public PredicateResults getsTheLock(CuratorFramework client, List<String> children, String sequenceNodeName, int maxLeases) throws Exception
                {
                    return readLockPredicate(children, sequenceNodeName);
                }
            }
        );
    }

    /**
     * Returns the lock used for reading.
     *
     * @return read lock
     */
    public InterProcessMutex     readLock()
    {
        return readMutex;
    }

    /**
     * Returns the lock used for writing.
     *
     * @return write lock
     */
    public InterProcessMutex     writeLock()
    {
        return writeMutex;
    }

    /**
     * 测试一个节点能否获取读锁。
     *
     * @param children 锁路径下的所有节点
     * @param sequenceNodeName 要测试的节点 - 代表一个读请求或写请求
     * @return 如果可以获得读锁则返回true
     * @throws Exception error
     */
    private PredicateResults readLockPredicate(List<String> children, String sequenceNodeName) throws Exception
    {
        // 如果已获得了写锁，那么允许获取读锁（再释放写锁）
        if ( writeMutex.isOwnedByCurrentThread() )
        {
            return new PredicateResults(null, true);
        }
        // 找到第一个申请写锁的节点。
        int         index = 0;
        int         firstWriteIndex = Integer.MAX_VALUE;
        int         ourIndex = -1;
        for ( String node : children )
        {
            if ( node.contains(WRITE_LOCK_NAME) )
            {
                // 获取第一个写请求节点的索引
                firstWriteIndex = Math.min(index, firstWriteIndex);
            }
            else if ( node.startsWith(sequenceNodeName) )
            {
                // 当前节点所在的索引
                ourIndex = index;
                break;
            }

            ++index;
        }
        StandardLockInternalsDriver.validateOurIndex(sequenceNodeName, ourIndex);

        // 如果当前节点在第一个写请求之前，那么可以获得读锁，否则监听第一个写锁的释放
        // 这里并不是监听它前面的最后一个写请求，其实监听它前面的最后一个写请求应该更好一点？
        boolean     getsTheLock = (ourIndex < firstWriteIndex);
        String      pathToWatch = getsTheLock ? null : children.get(firstWriteIndex);
        return new PredicateResults(pathToWatch, getsTheLock);
    }
}
