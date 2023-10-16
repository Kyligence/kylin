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


CREATE TABLE IF NOT EXISTS KE_IDENTIFIED_job_lock (
  id bigint(20) NOT NULL AUTO_INCREMENT,
  lock_id varchar(100) NOT NULL COMMENT 'what is locked',
  lock_node varchar(50) DEFAULT NULL COMMENT 'who locked it',
  lock_expire_time timestamp COMMENT 'when does the lock expire',
  priority integer DEFAULT 3,
  create_time bigint,
  update_time bigint,
  PRIMARY KEY (id),
  UNIQUE KEY uk_lock_id (lock_id)
) AUTO_INCREMENT=10000 ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
