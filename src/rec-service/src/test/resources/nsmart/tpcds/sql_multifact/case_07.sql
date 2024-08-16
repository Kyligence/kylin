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
select  i_item_id, 
        avg(ss.ss_quantity) agg1,
        avg(ss.ss_list_price) agg2,
        avg(ss.ss_coupon_amt) agg3,
        avg(ss.ss_sales_price) agg4 
 from TPCDS_BIN_PARTITIONED_ORC_2.store_sales as ss 
 join TPCDS_BIN_PARTITIONED_ORC_2.customer_demographics on ss.ss_cdemo_sk = customer_demographics.cd_demo_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.date_dim on ss.ss_sold_date_sk = date_dim.d_date_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.item on ss.ss_item_sk = item.i_item_sk
 join TPCDS_BIN_PARTITIONED_ORC_2.promotion on ss.ss_promo_sk = promotion.p_promo_sk
 where cd_gender = 'F' and 
       cd_marital_status = 'W' and
       cd_education_status = 'Primary' and
       (p_channel_email = 'N' or p_channel_event = 'N') and
       d_year = 1998 
 group by i_item_id
 order by i_item_id
 limit 100;
