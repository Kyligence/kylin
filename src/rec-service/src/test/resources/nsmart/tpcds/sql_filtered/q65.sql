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
-- SQL q65.sql
select 
    s_store_name,
    i_item_desc,
    sc.revenue,
    i_current_price,
    i_wholesale_cost,
    i_brand
from
    (select 
        ss_store_sk, avg(revenue) as ave
    from
        (select 
        ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
    from
        store_sales 
    join date_dim on ss_sold_date_sk = d_date_sk
    where d_month_seq between 1212 and 1212 + 11
    group by ss_store_sk , ss_item_sk) sa
    group by ss_store_sk) sb 
    join 
    (select 
        ss_store_sk, ss_item_sk, sum(ss_sales_price) as revenue
    from
        store_sales 
    join date_dim on ss_sold_date_sk = d_date_sk
    where d_month_seq between 1212 and 1212 + 11
    group by ss_store_sk , ss_item_sk) sc
    on sb.ss_store_sk = sc.ss_store_sk
    join store on s_store_sk = sc.ss_store_sk
    join item on i_item_sk = sc.ss_item_sk
where sc.revenue <= 0.1 * sb.ave
order by s_store_name , i_item_desc
limit 100
