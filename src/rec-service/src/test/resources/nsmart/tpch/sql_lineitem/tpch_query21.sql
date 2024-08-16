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
-- This query identifies certain suppliers who were not able to ship required parts in a timely manner.
-- Count distinct suppkey from lineitem, filter by order status, receiptdeplayed, nation name

select s_name, count(*) as numwait
from
(
    select
        l1.l_suppkey,
        s_name,
        l1.l_orderkey
    from
        v_lineitem l1
        inner join v_orders on l1.l_orderkey = o_orderkey
        inner join supplier on l1.l_suppkey = s_suppkey
        inner join nation on s_nationkey = n_nationkey
        inner join (
            select
                l_orderkey,
                count (distinct l_suppkey)
            from
                v_lineitem inner join v_orders on l_orderkey = o_orderkey
            where
                o_orderstatus = 'F'
            group by
                l_orderkey
            having
                count (distinct l_suppkey) > 1
        ) l2 on l1.l_orderkey = l2.l_orderkey
        inner join (
            select
                l_orderkey,
                count (distinct l_suppkey)
            from
                v_lineitem inner join v_orders on l_orderkey = o_orderkey
            where
                o_orderstatus = 'F'
                and l_receiptdelayed = 1
            group by
                l_orderkey
            having
                count (distinct l_suppkey) = 1
        ) l3 on l1.l_orderkey = l3.l_orderkey
    where
        o_orderstatus = 'F'
        and l_receiptdelayed = 1
        and n_name = 'SAUDI ARABIA'
    group by
        l1.l_suppkey,
        s_name,
        l1.l_orderkey
)
group by
    s_name
order by
    numwait desc,
    s_name
limit 100;
