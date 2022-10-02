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
-- KE-19841 optimize non-equal join with is not distinct from condition
SELECT SUM(PRICE),
       CAL_DT
FROM TEST_KYLIN_FACT
LEFT JOIN
  (SELECT TRANS_ID, ORDER_ID
   FROM TEST_KYLIN_FACT
   WHERE TRANS_ID > 100000000 group by TRANS_ID, ORDER_ID) FACT
ON (TEST_KYLIN_FACT.TRANS_ID = FACT.TRANS_ID
or (TEST_KYLIN_FACT.TRANS_ID is null and FACT.TRANS_ID is null))
and (TEST_KYLIN_FACT.ORDER_ID = FACT.ORDER_ID
or (TEST_KYLIN_FACT.ORDER_ID is null and FACT.ORDER_ID is null))
GROUP BY CAL_DT