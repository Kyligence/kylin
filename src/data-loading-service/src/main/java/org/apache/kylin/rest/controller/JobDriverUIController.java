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

package org.apache.kylin.rest.controller;

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.kylin.rest.util.JobDriverUIUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping(value = JobDriverUIUtil.DRIVER_UI_BASE)
public class JobDriverUIController {

    @Autowired
    @Qualifier("JobDriverUIUtil")
    private JobDriverUIUtil jobDriverUIUtil;

    @ApiOperation(value = "proxy", tags = { "DW" })
    @RequestMapping(value = "/{project}/{job_step}/**")
    @ResponseBody
    public void proxy(@PathVariable(value = "project") String project, @PathVariable(value = "job_step") String jobStep,
            HttpServletRequest request, HttpServletResponse response) throws IOException {
        jobDriverUIUtil.proxy(project, jobStep, request, response);
    }
}
