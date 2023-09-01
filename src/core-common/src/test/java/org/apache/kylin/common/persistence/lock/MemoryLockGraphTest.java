/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.common.persistence.lock;

import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
class MemoryLockGraphTest {

    @Test
    void deadLockCheckTest() throws InterruptedException {
        MemoryLockGraph graph = new MemoryLockGraph();
        MockedLockManager lockManager = new MockedLockManager(5, graph);
        lockManager.addLock(graph, "R_r5", "R_r1", "R_r2", "R_r3", "R_r3,R_r4");
        lockManager.addLock(graph, "W_r1", "W_r2", "W_r3", "W_r4", "W_r5");
        List<List<Long>> cycles = graph.checkForDeadLock();
        cycles.sort((l1, l2) -> l2.size() - l1.size());
        Assertions.assertEquals(2, cycles.size());
        List<Long> ids = lockManager.getThreadIds();
        Assertions.assertEquals(ids.size(), cycles.get(0).size());
        Assertions.assertTrue(cycles.get(0).containsAll(ids));
        Assertions.assertEquals(4, cycles.get(1).size());
        Assertions.assertTrue(cycles.get(1).containsAll(Arrays.asList(ids.get(0), ids.get(1), ids.get(2), ids.get(4))));
        lockManager.interruptAllThread();
    }

    @Test
    void getKeyNodeForDeadLockTest() throws InterruptedException {
        MemoryLockGraph graph = new MemoryLockGraph();
        MockedLockManager lockManager = new MockedLockManager(5, graph);
        lockManager.addLock(graph, "", "R_r4,R_r1", "R_r4,R_r2", "", "");
        lockManager.addLock(graph, "", "", "", "R_r3,R_r5", "R_r3");
        lockManager.addLock(graph, "W_r1", "W_r2", "W_r3", "W_r4", "W_r5");
        List<List<Long>> cycles = graph.checkForDeadLock();
        cycles.sort((l1, l2) -> l2.size() - l1.size());
        Assertions.assertEquals(4, cycles.size());
        Set<Long> nodesInCycle = graph.getKeyNodes(cycles);
        List<Long> ids = lockManager.getThreadIds();
        Assertions.assertArrayEquals(new Long[] { ids.get(3) }, nodesInCycle.toArray());
        lockManager.interruptAllThread();
    }

    @Test
    void preCheckTest() throws InterruptedException {
        MemoryLockGraph graph = new MemoryLockGraph();
        MockedLockManager lockManager = new MockedLockManager(5, graph);
        lockManager.addLock(graph, "", "R_r1", "R_r2", "R_r3", "R_r3");
        lockManager.addLock(graph, "W_r1", "W_r2", "W_r3", "", "");
        List<Long> allThreads = lockManager.getThreadIds();

        List<Long> cycle;
        cycle = graph.preCheck(allThreads.get(4), new HashSet<>(Collections.singletonList(allThreads.get(1))));
        Assertions.assertFalse(cycle.isEmpty());

        cycle = graph.preCheck(allThreads.get(0), new HashSet<>(Collections.singletonList(allThreads.get(3))));
        Assertions.assertTrue(cycle.isEmpty());

        lockManager.interruptAllThread();
    }

    static class MockedLockManager {
        List<Thread> threads = new ArrayList<>();
        List<ArrayBlockingQueue<String>> lockQueues = new ArrayList<>();

        public MockedLockManager(int cnt, MemoryLockGraph graph) {
            for (int i = 0; i < cnt; i++) {
                lockQueues.add(new ArrayBlockingQueue<>(10));
                int finalI = i;
                Thread t = new Thread(() -> {
                    long threadId = Thread.currentThread().getId();
                    String res;
                    List<TransactionLock> holdLocks = new ArrayList<>();
                    while (true) {
                        try {
                            res = lockQueues.get(finalI).take();
                            if (res.isEmpty()) {
                                continue;
                            }
                            String path = "/" + res.substring(2);
                            List<TransactionLock> locks = MemoryLockUtils.getPathLocks(path, res.startsWith("R"));
                            locks.forEach(lock -> {
                                graph.setThread(threadId, lock);
                                lock.lock();
                                graph.resetThread(threadId);
                                holdLocks.add(lock);
                            });
                        } catch (Exception ignored) {
                            holdLocks.forEach(TransactionLock::unlock);
                            break;
                        }
                    }
                });
                t.start();
                this.threads.add(t);
            }
        }

        public void addLock(MemoryLockGraph graph, String... resourceToLock) throws InterruptedException {
            for (int i = 0; i < resourceToLock.length; i++) {
                for (String res : resourceToLock[i].split(",")) {
                    this.lockQueues.get(i).add(res);
                }
            }
            // wait thread to take
            for (int i = 0; i < threads.size(); i++) {
                int finalI = i;
                await().atMost(1, TimeUnit.SECONDS).until(() -> lockQueues.get(finalI).isEmpty()
                        || graph.isThreadWaitForLock(threads.get(finalI).getId()));
            }
        }

        public void interruptAllThread() {
            this.threads.forEach(Thread::interrupt);
        }

        public List<Long> getThreadIds() {
            return this.threads.stream().map(Thread::getId).collect(Collectors.toList());
        }
    }
}
