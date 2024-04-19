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

package org.apache.kylin.common.util;

import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.guava30.shaded.common.util.concurrent.ThreadFactoryBuilder;

/**
 * methods copy from 'org.apache.kylin.engine.spark.utils.ThreadUtils', to fix module cycle dependency
 */
public class ThreadUtils {

    private static final String NAME_SUFFIX = "-%d";

    public static ThreadFactory newDaemonThreadFactory(String nameFormat) {
        return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
    }

    public static ScheduledExecutorService newDaemonSingleThreadScheduledExecutor(String threadName) {
        ThreadFactory factory = newDaemonThreadFactory(threadName);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, factory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public static ScheduledExecutorService newDaemonThreadScheduledExecutor(int corePoolSize, String threadName) {
        ThreadFactory factory = newDaemonThreadFactory(threadName);
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(corePoolSize, factory);
        // By default, a cancelled task is not automatically removed from the work queue until its delay
        // elapses. We have to enable it manually.
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    public static ThreadPoolExecutor newDaemonScalableThreadPool(String prefix, int corePoolSize, //
            int maximumPoolSize, //
            long keepAliveTime, //
            TimeUnit unit) {
        // Why not general BlockingQueue like LinkedBlockingQueue?
        // If there are more than corePoolSize but less than maximumPoolSize threads running,
        //  a new thread will be created only if the queue is full.
        // If we use unbounded queue, then maximumPoolSize will never be used.
        LinkedTransferQueue queue = new LinkedTransferQueue<Runnable>() {
            @Override
            public boolean offer(Runnable r) {
                return tryTransfer(r);
            }
        };
        ThreadFactory factory = newDaemonThreadFactory(prefix + NAME_SUFFIX);
        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, //
                keepAliveTime, unit, queue, factory);
        threadPool.setRejectedExecutionHandler(new RejectedExecutionHandler() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                try {
                    executor.getQueue().put(r);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        return threadPool;
    }

}
