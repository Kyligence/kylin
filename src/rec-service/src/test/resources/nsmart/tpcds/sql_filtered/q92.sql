--
-- Copyright (C) 2020 Kyligence Inc. All rights reserved.
--
-- http://kyligence.io
--
-- This software is the confidential and proprietary information of
-- Kyligence Inc. ("Confidential Information"). You shall not disclose
-- such Confidential Information and shall use it only in accordance
-- with the terms of the license agreement you entered into with
-- Kyligence Inc.
--
-- THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
-- "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
-- LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
-- A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
-- OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
-- SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
-- LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
-- DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
-- THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
-- (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
-- OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
--
-- SQL q92.sql
SELECT sum(case when ssci.customer_sk is not null and csci.customer_sk is null then 1
                                 else 0 end) as store_only,
               sum(case when ssci.customer_sk is null and csci.customer_sk is not null then 1
                                else 0 end) as catalog_only,
               sum(case when ssci.customer_sk is not null and csci.customer_sk is not null then 1 
                                 else 0 end) as store_and_catalog
FROM (SELECT ss.ss_customer_sk as customer_sk,
                             ss.ss_item_sk as item_sk
             FROM store_sales ss
             JOIN date_dim d1 ON (ss.ss_sold_date_sk = d1.d_date_sk)
             WHERE d1.d_month_seq >= 1206 and
                            d1.d_month_seq <= 1217
             GROUP BY ss.ss_customer_sk, ss.ss_item_sk) ssci
FULL OUTER JOIN (SELECT cs.cs_bill_customer_sk as customer_sk,
                                                   cs.cs_item_sk as item_sk
                                   FROM catalog_sales cs
                                   JOIN date_dim d2 ON (cs.cs_sold_date_sk = d2.d_date_sk)
                                   WHERE d2.d_month_seq >= 1206 and
                                                  d2.d_month_seq <= 1217
                                   GROUP BY cs.cs_bill_customer_sk, cs.cs_item_sk) csci
ON (ssci.customer_sk=csci.customer_sk and
        ssci.item_sk = csci.item_sk)