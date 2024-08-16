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
-- SQL q87.sql
select count(*) 
from (select distinct c_last_name as l1, c_first_name as f1, d_date as d1
       from store_sales
        JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
        JOIN customer ON store_sales.ss_customer_sk = customer.c_customer_sk
       where 
         d_month_seq between 1193 and 1193+11
  ) t1
      LEFT OUTER JOIN
      ( select distinct c_last_name as l2, c_first_name as f2, d_date as d2
       from catalog_sales
        JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
        JOIN customer ON catalog_sales.cs_bill_customer_sk = customer.c_customer_sk
       where 
         d_month_seq between 1193 and 1193+11
  ) t2
      ON t1.l1 = t2.l2 and
       t1.f1 = t2.f2 and
       t1.d1 = t2.d2
      LEFT OUTER JOIN
      (select distinct c_last_name as l3, c_first_name as f3, d_date as d3
       from web_sales
        JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
        JOIN customer ON web_sales.ws_bill_customer_sk = customer.c_customer_sk
       where 
         d_month_seq between 1193 and 1193+11
  ) t3
      ON t1.l1 = t3.l3 and
       t1.f1 = t3.f3 and
       t1.d1 = t3.d3
WHERE
    l2 is null and
    l3 is null