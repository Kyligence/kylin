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

package org.apache.kylin.common.constant;

import static org.apache.kylin.common.util.FileSystemUtil.OSS_FILE_SYSTEM_CLASS;
import static org.apache.kylin.common.util.FileSystemUtil.S3_FILE_SYSTEM_CLASS;

import java.util.Optional;

import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.guava30.shaded.common.base.Strings;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
@AllArgsConstructor
public enum ObsConfig {
    OSS("oss", "oss", "role", "oss_endpoint", "region", "fs.oss.bucket.%s.assumed.role.arn",
            "fs.oss.bucket.%s.credentials.provider", "fs.oss.bucket.%s.assumed.role.credentials.provider",
            "com.aliyun.oss.common.auth.STSAssumeRoleSessionCredentialsProvider",
            "com.aliyuncs.auth.InstanceProfileCredentialsProvider", "fs.oss.bucket.%s.endpoint",
            "fs.oss.bucket.%s.region"),

    S3("s3", "s3a", "role", "s3_endpoint", "region", "fs.s3a.bucket.%s.assumed.role.arn",
            "fs.s3a.bucket.%s.aws.credentials.provider", "fs.s3a.bucket.%s.assumed.role.credentials.provider",
            "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider", "fs.s3a.bucket.%s.endpoint",
            "fs.s3a.bucket.%s.region"),;

    // name of the storage, e.g. oss, s3
    private final String name;
    // type of the storage, e.g. oss, s3a
    private final String type;
    // role of the storage, e.g. role
    private final String rolePropertiesKey;
    // endpoint of the storage, e.g. s3_endpoint
    private final String endpointPropertiesKey;
    // region of the storage, e.g. region
    private final String regionPropertiesKey;
    // hadoop configuration key of the role arn
    private final String roleArnKey;
    // hadoop configuration key of the credential provider
    private final String credentialProviderKey;
    // hadoop configuration key of the assumed role credential provider
    private final String assumedRoleCredentialProviderKey;
    // hadoop configuration key of the credential provider class
    private final String credentialProviderValue;
    // hadoop configuration key of the assumed role credential provider class
    private final String assumedRoleCredentialProviderValue;
    // hadoop configuration key of the endpoint
    private final String endpointKey;
    // hadoop configuration key of the region
    private final String regionKey;

    public static Optional<ObsConfig> getByType(String type) {
        if (Strings.isNullOrEmpty(type)) {
            return Optional.empty();
        }
        for (ObsConfig obsConfig : ObsConfig.values()) {
            if (obsConfig.getType().equalsIgnoreCase(type)) {
                return Optional.of(obsConfig);
            }
        }
        return Optional.empty();
    }

    public static Optional<ObsConfig> getByLocation(String location) {
        if (Strings.isNullOrEmpty(location)) {
            return Optional.empty();
        }
        for (ObsConfig obsConfig : ObsConfig.values()) {
            // Only match the prefix, adapt to the fs configuration in the form of s3a and s3n
            if (location.startsWith(obsConfig.getName())) {
                return Optional.of(obsConfig);
            }
        }
        // Parse if used by the default configuration
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            String className = fs.getClass().getName();
            if (className.endsWith(S3_FILE_SYSTEM_CLASS)) {
                return Optional.of(S3);
            } else if (fs.getClass().getName().endsWith(OSS_FILE_SYSTEM_CLASS)) {
                return Optional.of(OSS);
            }
        } catch (Exception e) {
            // when the file system is not available, return empty
            log.warn("Failed to get file system", e);
        }
        return Optional.empty();
    }
}
