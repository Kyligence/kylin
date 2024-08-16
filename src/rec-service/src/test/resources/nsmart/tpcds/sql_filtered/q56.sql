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
-- SQL q56.sql
with ss as (
  select i_item_id,sum(ss_ext_sales_price) total_sales
  from store_sales
  join date_dim on ss_sold_date_sk = d_date_sk
  join customer_address on ss_addr_sk = ca_address_sk
  join item on ss_item_sk = i_item_sk
  where item.i_item_id in (
    select i.i_item_id
    from item i
    where i_color in ('purple','burlywood','indian')
  )
  and     d_year                  = 2001
  and     d_moy                   = 1
  and     ca_gmt_offset           = -6 
  group by i_item_id
),
cs as (
  select i_item_id,sum(cs_ext_sales_price) total_sales
  from catalog_sales
  join date_dim on cs_sold_date_sk = d_date_sk
  join customer_address on cs_bill_addr_sk = ca_address_sk
  join item on cs_item_sk = i_item_sk
  where item.i_item_id in (
    select i.i_item_id
    from item i
    where i_color in ('purple','burlywood','indian')
  )
  and     d_year                  = 2001
  and     d_moy                   = 1
  and     ca_gmt_offset           = -6 
  group by i_item_id
),
ws as (
  select i_item_id,sum(ws_ext_sales_price) total_sales
  from web_sales
  join date_dim on ws_sold_date_sk = d_date_sk
  join customer_address on ws_bill_addr_sk = ca_address_sk
  join item on ws_item_sk = i_item_sk
  where item.i_item_id in (
    select i.i_item_id
    from item i
    where i_color in ('purple','burlywood','indian')
  )
  and     d_year                  = 2001
  and     d_moy                   = 1
  and     ca_gmt_offset           = -6
  group by i_item_id
)
select  i_item_id ,sum(total_sales) total_sales
from (
    select * from ss 
    union all
    select * from cs 
    union all
    select * from ws
    ) tmp1
group by i_item_id
order by total_sales
limit 100