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
select avg(ss_quantity)
       ,avg(ss_ext_sales_price)
       ,avg(ss_ext_wholesale_cost)
       ,sum(ss_ext_wholesale_cost)
 from store_sales
 join store on store.s_store_sk = store_sales.ss_store_sk
 join customer_demographics on customer_demographics.cd_demo_sk = store_sales.ss_cdemo_sk
 join household_demographics on store_sales.ss_hdemo_sk=household_demographics.hd_demo_sk
 join customer_address on store_sales.ss_addr_sk = customer_address.ca_address_sk
 join date_dim on store_sales.ss_sold_date_sk = date_dim.d_date_sk
 where date_dim.d_year = 2001
 and((customer_demographics.cd_marital_status = 'M'
  and customer_demographics.cd_education_status = '4 yr Degree'
  and store_sales.ss_sales_price between 100.00 and 150.00
  and household_demographics.hd_dep_count = 3   
     )or
     (customer_demographics.cd_marital_status = 'D'
  and customer_demographics.cd_education_status = 'Primary'
  and store_sales.ss_sales_price between 50.00 and 100.00   
  and household_demographics.hd_dep_count = 1
     ) or 
     (customer_demographics.cd_marital_status = 'U'
  and customer_demographics.cd_education_status = 'Advanced Degree'
  and store_sales.ss_sales_price between 150.00 and 200.00 
  and household_demographics.hd_dep_count = 1  
     ))
 and((customer_address.ca_country = 'United States'
  and customer_address.ca_state in ('KY', 'GA', 'NM')
  and store_sales.ss_net_profit between 100 and 200  
     ) or
     (customer_address.ca_country = 'United States'
  and customer_address.ca_state in ('MT', 'OR', 'IN')
  and store_sales.ss_net_profit between 150 and 300  
     ) or
     (customer_address.ca_country = 'United States'
  and customer_address.ca_state in ('WI', 'MO', 'WV')
  and store_sales.ss_net_profit between 50 and 250  
     ))
;
