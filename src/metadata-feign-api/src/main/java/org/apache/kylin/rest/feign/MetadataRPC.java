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

package org.apache.kylin.rest.feign;

import java.util.List;

import org.apache.kylin.job.execution.MergerInfo;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.aspect.WaitForSyncAfterRPC;
import org.apache.kylin.rest.request.DataFlowUpdateRequest;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@FeignClient(name = "common", path = "/kylin/api/models/feign")
public interface MetadataRPC extends MetadataContract {

    @PostMapping(value = "/merge_metadata")
    List<NDataLayout[]> mergeMetadata(@RequestParam("project") String project, @RequestBody MergerInfo mergerInfo);

    @PostMapping(value = "/make_segment_ready")
    void makeSegmentReady(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
            @RequestParam("segmentId") String segmentId,
            @RequestParam("errorOrPausedJobCount") int errorOrPausedJobCount);

    @PostMapping(value = "/merge_metadata_for_sampling_or_snapshot")
    void mergeMetadataForSamplingOrSnapshot(@RequestParam("project") String project,
            @RequestBody MergerInfo mergerInfo);

    @PostMapping(value = "/merge_metadata_for_loading_internal_table")
    void mergeMetadataForLoadingInternalTable(@RequestParam("project") String project,
            @RequestBody MergerInfo mergerInfo);

    @PostMapping(value = "/check_and_auto_merge_segments")
    void checkAndAutoMergeSegments(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
            @RequestParam("owner") String owner);

    @PostMapping(value = "/update_dataflow")
    @WaitForSyncAfterRPC
    void updateDataflow(@RequestBody DataFlowUpdateRequest dataFlowUpdateRequest);

    @PostMapping(value = "/update_dataflow_maxBucketId")
    @WaitForSyncAfterRPC
    void updateDataflow(@RequestParam("project") String project, @RequestParam("dfId") String dfId,
            @RequestParam("segmentId") String segmentId, @RequestParam("maxBucketId") long maxBucketIt);

    @PostMapping(value = "/update_dataflow_status")
    @WaitForSyncAfterRPC
    void updateDataflowStatus(@RequestParam("project") String project, @RequestParam("uuid") String uuid,
            @RequestParam("status") RealizationStatusEnum status);

    @PostMapping(value = "/update_recommendations_count")
    @WaitForSyncAfterRPC
    void updateRecommendationsCount(@RequestParam("project") String project, @RequestParam("modelId") String modelId,
            @RequestParam("size") int size);
}
