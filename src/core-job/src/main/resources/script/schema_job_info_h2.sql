--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
--

CREATE TABLE IF NOT EXISTS KE_IDENTIFIED_job_info (
  id bigint(10) NOT NULL AUTO_INCREMENT,
  job_id varchar(100) NOT NULL,
  job_type varchar(50) NOT NULL,
  job_status varchar(50) NOT NULL,
  project varchar(100) NOT NULL,
  subject varchar(128) NOT NULL,
  model_id varchar(100),
  priority integer DEFAULT 3,
  job_content longblob NOT NULL,
  mvcc bigint(10),
  create_time bigint,
  update_time bigint,
  job_duration_millis bigint(10) NOT NULL DEFAULT '0' COMMENT 'total duration milliseconds',
   PRIMARY KEY (id),
   UNIQUE KEY uk_job_id (job_id)
) DEFAULT CHARSET=utf8;