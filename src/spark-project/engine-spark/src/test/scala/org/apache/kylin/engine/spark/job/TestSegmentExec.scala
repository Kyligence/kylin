/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.apache.kylin.engine.spark.job

import org.apache.kylin.engine.spark.scheduler.JobRuntime
import org.apache.kylin.metadata.model.NDataModel
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.metadata.model.TblColRef
import org.apache.spark.sql.SparkSession
import org.apache.spark.tracker.BuildContext
import org.scalatest.PrivateMethodTester
import org.scalatest.funsuite.AnyFunSuite

import java.util.concurrent.TimeUnit

class TestSegmentExec extends AnyFunSuite with PrivateMethodTester {

  private val myexec = new SegmentExec {
    override protected val jobId: String = ""
    override protected val project: String = ""
    override protected val segmentId: String = ""
    override protected val dataflowId: String = ""
    override protected val config: KylinConfig = null
    override protected val sparkSession: SparkSession = null
    override protected val dataModel: NDataModel = null
    override protected val storageType: Int = 0
    override protected val resourceContext: BuildContext = null
    override protected val runtime: JobRuntime = null

    override protected def columnIdFunc(colRef: TblColRef): String = ""

    override protected val sparkSchedulerPool: String = ""
  }

  test("test handle failure") {
    val func1 = PrivateMethod[Unit]('handleFailure)
    // test null
    val param1 = null
    myexec invokePrivate func1(param1)

    // test none
    val param2 = None
    myexec invokePrivate func1(param2)

    // test failure
    val param3 = Some(new Exception("test failure 1"))
    assertThrows[Exception](myexec invokePrivate func1(param3))
  }

  test("test fail fast poll") {
    val func1 = PrivateMethod[Int]('failFastPoll)
    // test illegal argument
    assertThrows[AssertionError](myexec invokePrivate func1(0L, TimeUnit.SECONDS))
  }
}
