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
package org.apache.kylin.common.persistence;

import java.nio.charset.Charset;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.lock.DeadLockException;
import org.apache.kylin.common.persistence.lock.MemoryLockUtils;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.guava30.shaded.common.base.Throwables;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.val;

@MetadataInfo(onlyProps = true)
class UnitOfWorkTest {

    @Test
    void testTransaction() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            MemoryLockUtils.lockAndRecord("/_global/path/res", null, false);
            MemoryLockUtils.lockAndRecord("/_global/path/res2", null, false);
            MemoryLockUtils.lockAndRecord("/_global/path/res3", null, false);
            resourceStore.checkAndPutResource("/_global/path/res",
                    ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
            resourceStore.checkAndPutResource("/_global/path/res2",
                    ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
            resourceStore.checkAndPutResource("/_global/path/res3",
                    ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path/res").getMvcc());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path/res2").getMvcc());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path/res3").getMvcc());
    }

    @Test
    void testExceptionInTransactionWithRetry() {
        try {
            UnitOfWork.doInTransactionWithRetry(() -> {
                val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                MemoryLockUtils.lockAndRecord("/_global/path/res", null, false);
                MemoryLockUtils.lockAndRecord("/_global/path/res2", null, false);
                resourceStore.checkAndPutResource("/_global/path/res",
                        ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                resourceStore.checkAndPutResource("/_global/path/res2",
                        ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                throw new IllegalArgumentException("surprise");
            }, UnitOfWork.GLOBAL_UNIT);
        } catch (Exception ignore) {
        }

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assertions.assertNull(resourceStore.getResource("/_global/path/res"));
        Assertions.assertNull(resourceStore.getResource("/_global/path/res2"));

        // test can be used again after exception
        testTransaction();
    }

    @Test
    void testUnitOfWorkPreprocess() {
        class A implements UnitOfWork.Callback<Object> {
            private final List<String> list = Lists.newArrayList();

            @Override
            public String toString() {
                return String.valueOf(list.size());
            }

            @Override
            public void preProcess() {
                try {
                    throw new Throwable("no args");
                } catch (Throwable e) {
                    list.add(e.getMessage());
                }
            }

            @Override
            public Object process() {
                list.add(this.toString());
                throw new IllegalStateException("conflict");
            }

            @Override
            public void onProcessError(Throwable throwable) {
                list.add("conflict");
            }
        }
        A callback = new A();
        Assertions.assertTrue(callback.list.isEmpty());
        try {
            UnitOfWork.doInTransactionWithRetry(callback, UnitOfWork.GLOBAL_UNIT);
            Assertions.fail();
        } catch (Throwable e) {
            Assertions.assertTrue(e instanceof TransactionException);
            Assertions.assertEquals("conflict", Throwables.getRootCause(e).getMessage());
        }
        Assertions.assertEquals(7, callback.list.size());
        Assertions.assertEquals("no args", callback.list.get(0));
        Assertions.assertEquals("1", callback.list.get(1));
        Assertions.assertEquals("no args", callback.list.get(2));
        Assertions.assertEquals("3", callback.list.get(3));
        Assertions.assertEquals("no args", callback.list.get(4));
        Assertions.assertEquals("5", callback.list.get(5));
        Assertions.assertEquals("conflict", callback.list.get(6));
    }

    @Test
    void testReentrant() {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            MemoryLockUtils.lockAndRecord("/_global/path/res", null, false);
            MemoryLockUtils.lockAndRecord("/_global/path/res2", null, false);
            resourceStore.checkAndPutResource("/_global/path/res",
                    ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
            resourceStore.checkAndPutResource("/_global/path/res2",
                    ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
            UnitOfWork.doInTransactionWithRetry(() -> {
                val resourceStore2 = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                MemoryLockUtils.lockAndRecord("/_global/path2/res2/1", null, false);
                MemoryLockUtils.lockAndRecord("/_global/path2/res2/2", null, false);
                MemoryLockUtils.lockAndRecord("/_global/path2/res2/3", null, false);
                resourceStore2.checkAndPutResource("/_global/path2/res2/1",
                        ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                resourceStore2.checkAndPutResource("/_global/path2/res2/2",
                        ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                resourceStore2.checkAndPutResource("/_global/path2/res2/3",
                        ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                Assertions.assertEquals(resourceStore, resourceStore2);
                return 0;
            }, UnitOfWork.GLOBAL_UNIT);
            MemoryLockUtils.lockAndRecord("/_global/path/res3", null, false);
            resourceStore.checkAndPutResource("/_global/path/res3",
                    ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
            return 0;
        }, UnitOfWork.GLOBAL_UNIT);

        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path/res").getMvcc());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path/res2").getMvcc());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path2/res2/1").getMvcc());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path2/res2/2").getMvcc());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path2/res2/3").getMvcc());
        Assertions.assertEquals(0, resourceStore.getResource("/_global/path/res3").getMvcc());
    }

    @Test
    void testReadLockExclusive() {
        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        resourceStore.checkAndPutResource("/_global/path/res1",
                ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
        Object condition = new Object();
        AtomicBoolean stop = new AtomicBoolean();
        Thread readLockHelder = new Thread(() -> {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(true).maxRetry(1).processor(() -> {
                        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                                .getResource("/_global/path/res1");
                        synchronized (condition) {
                            condition.notify();
                        }
                        boolean interrupted = false;
                        while (!interrupted && !Thread.interrupted() && !stop.get()) {
                            synchronized (condition) {
                                condition.notify();
                            }
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                interrupted = true;
                            }
                        }
                        return 0;
                    }).build());
        });
        readLockHelder.start();
        synchronized (condition) {
            try {
                condition.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        long readStart = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(true).maxRetry(1).processor(() -> {
                        long cost = System.currentTimeMillis() - readStart;
                        Assertions.assertTrue(cost < 500);
                        Assertions.assertEquals(0, ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                                .getResource("/_global/path/res1").getMvcc());
                        return 0;
                    }).build());
        } catch (Exception e) {
            Assertions.fail();
        }
        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stop.set(true);
        }).start();
        long writeStart = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(false).maxRetry(1).processor(() -> {
                        MemoryLockUtils.lockAndRecord("/_global/path/res1", null, false);
                        long cost = System.currentTimeMillis() - writeStart;
                        Assertions.assertTrue(cost > 1500);
                        Assertions.assertEquals(0, ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                                .getResource("/_global/path/res1").getMvcc());
                        return 0;
                    }).build());
        } catch (Exception e) {
            Assertions.fail();
        }
        stop.set(true);
    }

    @Test
    void testWriteLockExclusive() {
        Object condition = new Object();
        AtomicBoolean stop = new AtomicBoolean();
        Thread writeLockHelder = new Thread(() -> {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(false).maxRetry(1).processor(() -> {
                        val resourceStoreInTransaction = ResourceStore
                                .getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                        System.out.println("Write thread start to lock");
                        MemoryLockUtils.lockAndRecord("/_global/path/res1", null, false);
                        resourceStoreInTransaction.checkAndPutResource("/_global/path/res1",
                                ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                        synchronized (condition) {
                            condition.notify();
                        }
                        boolean interrupted = false;
                        while (!interrupted && !Thread.interrupted() && !stop.get()) {
                            synchronized (condition) {
                                condition.notify();
                            }
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                interrupted = true;
                            }
                        }
                        synchronized (condition) {
                            condition.notify();
                        }
                        System.out.println("Write thread finished.");
                        return 0;
                    }).build());
        });
        writeLockHelder.start();
        synchronized (condition) {
            try {
                condition.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            stop.set(true);
        }).start();
        long start = System.currentTimeMillis();
        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT) //
                .readonly(true).maxRetry(1).processor(() -> {
                    System.out.println("Read thread start to lock.");
                    RawResource raw = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                            .getResource("/_global/path/res1");
                    System.out.println("Read thread lock succeed.");
                    long cost = System.currentTimeMillis() - start;
                    System.out.println(cost + " " + (raw == null ? -2 : raw.getMvcc()));
                    Assertions.assertTrue(cost > 1500);
                    assert raw != null;
                    Assertions.assertEquals(0, raw.getMvcc());
                    return 0;
                }).build());
        stop.set(true);
    }

    @OverwriteProp(key = "kylin.env", value = "PROD")
    @Test
    void testUpdateInReadTransaction() {
        try {
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT)
                    .readonly(true).maxRetry(1).processor(() -> {
                        val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                        MemoryLockUtils.lockAndRecord("/_global/path/res1", null, false);
                        resourceStore.checkAndPutResource("/_global/path/res1",
                                ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                        return 0;
                    }).build());
            Assertions.fail();
        } catch (Exception e) {
            Assertions.assertEquals(TransactionException.class, e.getClass());
        }
    }

    @Test
    public void testReadTransaction() {
        ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource("/_global/path/res1",
                ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
        UnitOfWork.doInTransactionWithRetry(
                UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT).readonly(true).maxRetry(1).processor(() -> {
                    val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    Assertions.assertEquals(0, resourceStore.getResource("/_global/path/res1").getMvcc());
                    return 0;
                }).build());
    }

    @Test
    public void testWriteTransaction() {

        UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().unitName(UnitOfWork.GLOBAL_UNIT).readonly(false)
                .maxRetry(1).processor(() -> {
                    val resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
                    MemoryLockUtils.lockAndRecord("/_global/path/res1", null, false);
                    resourceStore.checkAndPutResource("/_global/path/res1",
                            ByteSource.wrap("{}".getBytes(Charset.defaultCharset())), -1L);
                    return 0;
                }).build());
        Assertions.assertEquals(0, ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getResource("/_global/path/res1").getMvcc());

    }

    @Test
    void testRetryMoreTimeForDeadLockException() {
        KylinConfig.getInstanceFromEnv().setProperty("kylin.env.max-seconds-for-dead-lock-retry", "2");
        long startTime = System.currentTimeMillis();
        try {
            UnitOfWork.doInTransactionWithRetry(
                    UnitOfWorkParams.builder().retryMoreTimeForDeadLockException(true).processor(() -> {
                        throw new DeadLockException("test");
                    }).build());
        } catch (Exception e) {
            Assertions.assertEquals(DeadLockException.class, e.getCause().getClass());
            Assertions.assertTrue(System.currentTimeMillis() - startTime > 2 * 1000);
        }
    }
}
