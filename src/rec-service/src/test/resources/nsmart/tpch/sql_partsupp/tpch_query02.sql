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
--  Get min ps_supplycost from v_partsupp. And query some details filter by p_size/p_type/r_name.

with q2_min_ps_supplycost as (
	select
		p_partkey as min_p_partkey,
		min(ps_supplycost) as min_ps_supplycost
	from
		v_partsupp
		inner join part on p_partkey = ps_partkey
		inner join supplier on s_suppkey = ps_suppkey
		inner join nation on s_nationkey = n_nationkey
		inner join region on n_regionkey = r_regionkey
	where
		r_name = 'EUROPE'
	group by
		p_partkey
)
select
	s_acctbal,
	s_name,
	n_name,
	p_partkey,
	p_mfgr,
	s_address,
	s_phone,
	s_comment
from
	v_partsupp
	inner join part on p_partkey = ps_partkey
	inner join supplier on s_suppkey = ps_suppkey
	inner join nation on s_nationkey = n_nationkey
	inner join region on n_regionkey = r_regionkey
	inner join q2_min_ps_supplycost on ps_supplycost = min_ps_supplycost and p_partkey = min_p_partkey
where
	p_size = 37
	and p_type like '%COPPER'
	and r_name = 'EUROPE'	
order by
	s_acctbal desc,
	n_name,
	s_name,
	p_partkey
limit 100;

