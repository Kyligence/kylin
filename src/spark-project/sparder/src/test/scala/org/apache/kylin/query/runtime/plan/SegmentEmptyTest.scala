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
package org.apache.kylin.query.runtime.plan

import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession, SparderBaseFunSuite}
import org.junit.Assert

import java.util

class SegmentEmptyTest extends SparderBaseFunSuite with SharedSparkSession with LocalMetadata {

    val prunedSegment1 = null
    val prunedSegment2 = new util.LinkedList[NDataSegment]
    val prunedSegment3 = new util.LinkedList[NDataSegment]
    prunedSegment3.add(new NDataSegment())

    val prunedStreamingSegment1 = null
    val prunedStreamingSegment2 = new util.LinkedList[NDataSegment]
    val prunedStreamingSegment3 = new util.LinkedList[NDataSegment]
    prunedStreamingSegment3.add(new NDataSegment())

    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment1, prunedStreamingSegment1))
    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment1, prunedStreamingSegment2))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment1, prunedStreamingSegment3))

    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment2, prunedStreamingSegment1))
    Assert.assertTrue(TableScanPlan.isSegmentsEmpty(prunedSegment2, prunedStreamingSegment2))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment2, prunedStreamingSegment3))

    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment3, prunedStreamingSegment1))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment3, prunedStreamingSegment2))
    Assert.assertFalse(TableScanPlan.isSegmentsEmpty(prunedSegment3, prunedStreamingSegment3))
}
