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
-- SQL q09.sql
select bucket1, bucket2, bucket3, bucket4, bucket5
from
(select case when count1 > 62316685 then then1 else else1 end bucket1
from (
select count(*) count1, avg(ss_ext_sales_price) then1, avg(ss_net_paid_inc_tax) else1
from store_sales
where ss_quantity between 1 and 20
) A1) B1
CROSS JOIN
(select case when count2 > 19045798 then then2 else else2 end bucket2
from (
select count(*) count2, avg(ss_ext_sales_price) then2, avg(ss_net_paid_inc_tax) else2
from store_sales
where ss_quantity between 21 and 40
) A2) B2
CROSS JOIN
(select case when count3 > 365541424 then then3 else else3 end bucket3
from (
select count(*) count3, avg(ss_ext_sales_price) then3, avg(ss_net_paid_inc_tax) else3
from store_sales
where ss_quantity between 41 and 60
) A3) B3
CROSS JOIN
(select case when count4 > 216357808 then then4 else else4 end bucket4
from (
select count(*) count4, avg(ss_ext_sales_price) then4, avg(ss_net_paid_inc_tax) else4
from store_sales
where ss_quantity between 61 and 80
) A4) B4
CROSS JOIN
(select case when count5 > 184483884 then then5 else else5 end bucket5
from (
select count(*) count5, avg(ss_ext_sales_price) then5, avg(ss_net_paid_inc_tax) else5
from store_sales
where ss_quantity between 81 and 100
) A5) B5
CROSS JOIN
reason
where r_reason_sk = 1