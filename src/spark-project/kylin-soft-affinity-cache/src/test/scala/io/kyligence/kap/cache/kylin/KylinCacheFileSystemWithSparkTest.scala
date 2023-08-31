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

package io.kyligence.kap.cache.kylin

import io.kyligence.kap.cache.fs.CacheFileSystemConstants
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.{SparkFunSuite, TaskContext}

class KylinCacheFileSystemWithSparkTest extends SparkFunSuite with SharedSparkSession {

  protected override def afterEach(): Unit = {
    super.afterEach()
    sparkContext.setLocalProperty(CacheFileSystemConstants.PARAMS_KEY_LOCAL_CACHE_FOR_CURRENT_FILES, null)
    sparkContext.setLocalProperty(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME, null)
  }

  test("Test use local query for current executor") {
    val key = CacheFileSystemConstants.PARAMS_KEY_LOCAL_CACHE_FOR_CURRENT_FILES

    // TaskContext.get() == null
    {
      val fs: KylinCacheFileSystem = new KylinCacheFileSystem
      assert(!fs.isUseLocalCacheForCurrentExecutor)
    }

    // spark.kylin.local-cache.for.current.files is empty
    {
      sparkContext.setLocalProperty(key, "")
      sparkContext.range(0, 1, numSlices = 1).foreachPartition { x =>
        val ctx = TaskContext.get()
        assert(ctx.getLocalProperty(key).isEmpty)
        val fs: KylinCacheFileSystem = new KylinCacheFileSystem
        assert(fs.isUseLocalCacheForCurrentExecutor)
      }
    }

    // spark.kylin.local-cache.for.current.files is "true"
    {
      sparkContext.setLocalProperty(key, "true")
      sparkContext.range(0, 1, numSlices = 1).foreachPartition { x =>
        val ctx = TaskContext.get()
        assert(ctx.getLocalProperty(key).eq("true"))
        val fs: KylinCacheFileSystem = new KylinCacheFileSystem
        assert(fs.isUseLocalCacheForCurrentExecutor)
      }
    }
  }

  test("Test set accept cache time") {
    val key = CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME

    // first set
    {
      val acceptCacheTime = System.currentTimeMillis()
      KylinCacheFileSystem.setAcceptCacheTimeLocally(acceptCacheTime)
      val actual = java.lang.Long.parseLong(sparkContext.getLocalProperty(key))
      assert(actual == acceptCacheTime)
    }

    // second set, take the max
    {
      val acceptCacheTime = System.currentTimeMillis() + 500L
      KylinCacheFileSystem.setAcceptCacheTimeLocally(acceptCacheTime)
      val actual = java.lang.Long.parseLong(sparkContext.getLocalProperty(key))
      assert(actual == acceptCacheTime)
    }

    // third set, time range exceeds
    {
      val acceptCacheTime = System.currentTimeMillis() + 2000L
      KylinCacheFileSystem.setAcceptCacheTimeLocally(acceptCacheTime)
      val actual = java.lang.Long.parseLong(sparkContext.getLocalProperty(key))
      assert(actual < acceptCacheTime)
    }

    // clear and set to null, should close to current system time
    {
      KylinCacheFileSystem.clearAcceptCacheTimeLocally()
      KylinCacheFileSystem.setAcceptCacheTimeLocally(null)
      val actual = java.lang.Long.parseLong(sparkContext.getLocalProperty(key))
      assert(Math.abs(actual - System.currentTimeMillis()) < 1000)
    }
  }

  test("Test clear accept cache time") {
    val key = CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME

    // Clear accept cache time without TaskContext
    sparkContext.setLocalProperty(key, "123")
    KylinCacheFileSystem.clearAcceptCacheTimeLocally()
    assertResult(null)(sparkContext.getLocalProperty(key))
  }

  test("Test get accept cache time") {
    val key = CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME

    // get accept cache time inside task
    sparkContext.setLocalProperty(key, "123")
    sparkContext.range(0, 1, numSlices = 1).foreachPartition { x =>
      assert(KylinCacheFileSystem.getAcceptCacheTimeLocally == 123L)
    }

    sparkContext.setLocalProperty(key, null)
    sparkContext.range(0, 1, numSlices = 1).foreachPartition { x =>
      val now = System.currentTimeMillis()
      // time diff 300ms should be enough
      assert(Math.abs(KylinCacheFileSystem.getAcceptCacheTimeLocally - now) < 300L)
    }

    // get accept cache time on driver
    sparkContext.setLocalProperty(key, "456")
    sparkContext.range(0, 1, numSlices = 1).foreachPartition { x =>
      assert(KylinCacheFileSystem.getAcceptCacheTimeLocally == 456L)
    }

    sparkContext.setLocalProperty(key, null)
    sparkContext.range(0, 1, numSlices = 1).foreachPartition { x =>
      val now = System.currentTimeMillis()
      // time diff 300ms should be enough
      assert(Math.abs(KylinCacheFileSystem.getAcceptCacheTimeLocally - now) < 300L)
    }
  }

}
