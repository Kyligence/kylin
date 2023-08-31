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

package io.kyligence.kap.cache.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.kylin.guava30.shaded.common.util.concurrent.UncheckedExecutionException;
import org.junit.Test;


public class ManagerOfCacheFileStatusTest {

    @Test
    public void makeIOException() {
        {
            IOException ioException = new IOException("Test IOException");
            IOException result = ManagerOfCacheFileStatus.makeIOException(ioException);
            assertSame(ioException, result);
        }

        {
            IOException ioException = new IOException("Test IOException");
            ExecutionException executionException = new ExecutionException(ioException);
            IOException result = ManagerOfCacheFileStatus.makeIOException(executionException);
            assertSame(ioException, result);
        }

        {
            IOException ioException = new IOException("Test IOException");
            UncheckedExecutionException uncheckedExecutionException = new UncheckedExecutionException(ioException);
            IOException result = ManagerOfCacheFileStatus.makeIOException(uncheckedExecutionException);
            assertSame(ioException, result);
        }

        {
            IOException ioException = new IOException("Test IOException");
            Throwable wrappedException = new RuntimeException(ioException);
            IOException result = ManagerOfCacheFileStatus.makeIOException(wrappedException);
            assertSame(ioException, result);
        }

        {
            Throwable otherException = new RuntimeException("Test RuntimeException");
            IOException result = ManagerOfCacheFileStatus.makeIOException(otherException);
            assertEquals(IOException.class, result.getClass());
            assertEquals(otherException, result.getCause());
        }
    }
}
