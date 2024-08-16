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
--  For part like 'forest%' and supplier in CANADA, find suppliers whose stock is more than demand.

with tmp3 as (
    select l_partkey, 0.5 * sum(l_quantity) as sum_quantity, l_suppkey
    from v_lineitem
    inner join supplier on l_suppkey = s_suppkey
    inner join nation on s_nationkey = n_nationkey
    inner join part on l_partkey = p_partkey
    where l_shipdate >= '1994-01-01' and l_shipdate <= '1995-01-01'
    and n_name = 'CANADA'
    and p_name like 'forest%'
    group by l_partkey, l_suppkey
)

select
    s_name,
    s_address
from
    v_partsupp
    inner join supplier on ps_suppkey = s_suppkey
    inner join tmp3 on ps_partkey = l_partkey and ps_suppkey = l_suppkey
where
    ps_availqty > sum_quantity
group by
    s_name, s_address
order by
    s_name;
