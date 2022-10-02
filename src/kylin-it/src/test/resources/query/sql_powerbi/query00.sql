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
select count(*) as CNT, "LSTG_FORMAT_NAME" from "DEFAULT"."TEST_KYLIN_FACT" as "OTBL" inner join "DEFAULT"."TEST_ACCOUNT" as "ITBL" on (("OTBL"."SELLER_ID" = "ITBL"."ACCOUNT_ID" and "OTBL"."SELLER_ID" is not null) and "ITBL"."ACCOUNT_ID" is not null or "OTBL"."SELLER_ID" is null and "ITBL"."ACCOUNT_ID" is null)
inner join "EDW"."TEST_CAL_DT" as "DTBL" on (("OTBL"."CAL_DT" = "DTBL"."CAL_DT" and "OTBL"."CAL_DT" is not null) and "DTBL"."CAL_DT" is not null or "OTBL"."CAL_DT" is null and "DTBL"."CAL_DT" is null)
inner join "TEST_CATEGORY_GROUPINGS" AS "TEST_CATEGORY_GROUPINGS"
on "OTBL"."LEAF_CATEG_ID" = "TEST_CATEGORY_GROUPINGS"."LEAF_CATEG_ID" and "OTBL"."LSTG_SITE_ID" = "TEST_CATEGORY_GROUPINGS"."SITE_ID"
group by "LSTG_FORMAT_NAME"
