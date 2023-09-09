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

-- SQL not supported when there was RexCall in AggregateCall arguments.
select sum(LINEORDER.LO_EXTENDEDPRICE * 3),
       LINEORDER.LO_ORDERDATE,
       dt.DATE_TIME
from SSB.P_LINEORDER as LINEORDER
         inner join (select 19931014 as DATE_TIME) dt
                    on LINEORDER.LO_ORDERDATE = dt.DATE_TIME
group by LO_ORDERDATE, dt.DATE_TIME