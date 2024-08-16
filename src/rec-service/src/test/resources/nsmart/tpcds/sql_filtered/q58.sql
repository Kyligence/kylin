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
-- SQL q58.sql
select  ss_items.item_id
       ,ss_item_rev
       ,ss_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ss_dev
       ,cs_item_rev
       ,cs_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 cs_dev
       ,ws_item_rev
       ,ws_item_rev/(ss_item_rev+cs_item_rev+ws_item_rev)/3 * 100 ws_dev
       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
FROM
( select i_item_id item_id ,sum(ss_ext_sales_price) as ss_item_rev 
 from store_sales
     JOIN item ON store_sales.ss_item_sk = item.i_item_sk
     JOIN date_dim ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
     JOIN (select d1.d_date
                 from date_dim d1 JOIN date_dim d2 ON d1.d_week_seq = d2.d_week_seq
                 where d2.d_date = '1998-08-04') sub ON date_dim.d_date = sub.d_date
 group by i_item_id ) ss_items
JOIN
( select i_item_id item_id ,sum(cs_ext_sales_price) as cs_item_rev 
 from catalog_sales
     JOIN item ON catalog_sales.cs_item_sk = item.i_item_sk
     JOIN date_dim ON catalog_sales.cs_sold_date_sk = date_dim.d_date_sk
     JOIN (select d1.d_date
                 from date_dim d1 JOIN date_dim d2 ON d1.d_week_seq = d2.d_week_seq
                 where d2.d_date = '1998-08-04') sub ON date_dim.d_date = sub.d_date
 group by i_item_id ) cs_items
ON ss_items.item_id=cs_items.item_id
JOIN
( select i_item_id item_id ,sum(ws_ext_sales_price) as ws_item_rev 
 from web_sales
     JOIN item ON web_sales.ws_item_sk = item.i_item_sk
     JOIN date_dim ON web_sales.ws_sold_date_sk = date_dim.d_date_sk
     JOIN (select d1.d_date
                 from date_dim d1 JOIN date_dim d2 ON d1.d_week_seq = d2.d_week_seq
                 where d2.d_date = '1998-08-04') sub ON date_dim.d_date = sub.d_date
 group by i_item_id ) ws_items
ON ss_items.item_id=ws_items.item_id 
 where
       ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
 order by item_id ,ss_item_rev
 limit 100