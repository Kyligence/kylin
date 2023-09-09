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


SELECT li.C_NAME,
       count(li.LO_ORDERKEY) count_order,
       li.LO_ORDERDATE,
       dt1.DATE_TIME,
       li.DATE_TIME,
       sum(li.C_CUSTKEY)     sum_custkey
FROM (SELECT '1996-01-02' AS DATE_TIME) dt1
         LEFT JOIN
     (SELECT third.LO_ORDERDATE,
             third.LO_ORDERKEY,
             third.C_NAME,
             third.C_CUSTKEY,
             third.DATE_TIME
      FROM (SELECT dt2.LO_ORDERDATE,
                   dt2.LO_ORDERKEY,
                   dt2.C_NAME,
                   dt2.C_CUSTKEY,
                   dt4.DATE_TIME
            FROM (SELECT LINEORDER.LO_ORDERDATE,
                         LINEORDER.LO_ORDERKEY,
                         CUSTOMER.C_NAME,
                         CUSTOMER.C_CUSTKEY,
                         dt.DATE_TIME
                  FROM "SSB"."LINEORDER" AS "LINEORDER"
                           INNER JOIN "SSB"."CUSTOMER" AS "CUSTOMER"
                                      ON "LINEORDER"."LO_CUSTKEY" = "CUSTOMER"."C_CUSTKEY"
                           INNER JOIN
                       (SELECT '1995-03-29' AS DATE_TIME
                        UNION
                        SELECT '1995-04-01' AS DATE_TIME
                        UNION ALL
                        SELECT '1995-03-29' AS DATE_TIME
                        UNION ALL
                        SELECT '1995-04-01' AS DATE_TIME
                        UNION ALL
                        SELECT '1996-01-02' AS DATE_TIME
                        UNION
                        SELECT CASE WHEN 1 = 1 THEN '1995-06-01' ELSE '1995-07-01' END AS DATE_TIME) dt
                       ON LINEORDER.LO_ORDERDATE = dt.DATE_TIME) dt2
                     INNER JOIN
                     (SELECT '1996-01-02' AS DATE_TIME) dt4 ON dt2.LO_ORDERDATE = dt4.DATE_TIME) third) li
     ON dt1.DATE_TIME = li.LO_ORDERDATE
GROUP BY li.C_NAME,
         li.LO_ORDERDATE,
         dt1.DATE_TIME,
         li.DATE_TIME