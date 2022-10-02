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
-- agg push down count with inner col (left join)
SELECT
        COUNT(*)
    FROM
    (
            SELECT TEST_KYLIN_FACT.SELLER_ID + TEST_KYLIN_FACT.LSTG_SITE_ID AS S, CAL_DT
            FROM TEST_KYLIN_FACT
            ) B
    LEFT JOIN
        (  SELECT
            TEST_KYLIN_FACT.SELLER_ID,
            SUM(TEST_KYLIN_FACT.ITEM_COUNT),
            CAL_DT
        FROM
            TEST_KYLIN_FACT
        GROUP BY
            TEST_KYLIN_FACT.SELLER_ID,
            CAL_DT
            ) A
          ON A.SELLER_ID = B.S AND A.CAL_DT = B.CAL_DT
