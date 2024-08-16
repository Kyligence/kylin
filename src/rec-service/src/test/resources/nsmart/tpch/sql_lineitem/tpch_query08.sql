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
-- Calculate mkt share percent(sum saleprice) of some nation from lineitem, filter by region name, order date range, nation name
-- group by order year

with all_nations as (
    select
			o_orderyear as o_year,
			l_saleprice as volume,
			n2.n_name as nation
		from
		    v_lineitem
		    inner join part on l_partkey = p_partkey
		    inner join supplier on l_suppkey = s_suppkey
			inner join v_orders on l_orderkey = o_orderkey
			inner join customer on o_custkey = c_custkey
		    inner join nation n1 on c_nationkey = n1.n_nationkey
		    inner join nation n2 on s_nationkey = n2.n_nationkey
		    inner join region on n1.n_regionkey = r_regionkey
		where
			r_name = 'AMERICA'
			and o_orderdate between '1995-01-01' and '1996-12-31'
			and p_type = 'ECONOMY BURNISHED NICKEL'
),
peru as (
    select o_year, sum(volume) as peru_volume from all_nations where nation = 'PERU' group by o_year
),
all_data as (
    select o_year, sum(volume) as all_volume from all_nations group by o_year
)
select peru.o_year, peru_volume / all_volume as mkt_share from peru inner join all_data on peru.o_year = all_data.o_year;
