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
--  For certain countries (c_phone prefix), find customer who has no order, 
--  but account balance higher than avg, count such customers group by country.

with avg_tmp as (
    select
        avg(c_acctbal) as avg_acctbal
    from
        customer
    where
        c_acctbal > 0.00 and substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
),
cus_tmp as (
    select c_custkey as noordercus
    from
        customer left join v_orders on c_custkey = o_custkey
    where o_orderkey is null
)

select
    cntrycode,
    count(1) as numcust,
    sum(c_acctbal) as totacctbal
from (
    select
        substring(c_phone, 1, 2) as cntrycode,
        c_acctbal
    from 
        customer inner join cus_tmp on c_custkey = noordercus, avg_tmp
    where 
        substring(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
        and c_acctbal > avg_acctbal
) t
group by
    cntrycode
order by
    cntrycode;
