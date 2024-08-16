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
-- SQL q32.sql
SELECT sum(cs1.cs_ext_discount_amt) as excess_discount_amount
FROM (SELECT cs.cs_item_sk as cs_item_sk,
                             cs.cs_ext_discount_amt as cs_ext_discount_amt
             FROM catalog_sales cs
             JOIN date_dim d ON (d.d_date_sk = cs.cs_sold_date_sk)
             WHERE d.d_date between '2000-01-27' and '2000-04-27') cs1
JOIN item i ON (i.i_item_sk = cs1.cs_item_sk)
JOIN (SELECT cs2.cs_item_sk as cs_item_sk,
                          1.3 * avg(cs_ext_discount_amt) as avg_cs_ext_discount_amt
           FROM (SELECT cs.cs_item_sk as cs_item_sk,
                                        cs.cs_ext_discount_amt as cs_ext_discount_amt
                        FROM catalog_sales cs
                        JOIN date_dim d ON (d.d_date_sk = cs.cs_sold_date_sk)
                        WHERE d.d_date between '2000-01-27' and '2000-04-27') cs2
                        GROUP BY cs2.cs_item_sk) tmp1
ON (i.i_item_sk = tmp1.cs_item_sk)
WHERE i.i_manufact_id = 436 and
               cs1.cs_ext_discount_amt > tmp1.avg_cs_ext_discount_amt