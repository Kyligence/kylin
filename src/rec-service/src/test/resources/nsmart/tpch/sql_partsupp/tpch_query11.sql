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
-- This query finds the most important subset of suppliers' stock in a given nation.
-- Sum part value from partsupp, filter by nation name, part_value, group by partkey

with q11_part_tmp_cached as (
	select
		ps_partkey,
		sum(ps_partvalue) as part_value
	from
		v_partsupp
		inner join supplier on ps_suppkey = s_suppkey
		inner join nation on s_nationkey = n_nationkey
	where
		n_name = 'GERMANY'
	group by ps_partkey
),
q11_sum_tmp_cached as (
	select
		sum(part_value) as total_value
	from
		q11_part_tmp_cached
)

select
	ps_partkey, 
	part_value
from (
	select
		ps_partkey,
		part_value,
		total_value
	from
		q11_part_tmp_cached, q11_sum_tmp_cached
) a
where
	part_value > total_value * 0.0001
order by
	part_value desc;
