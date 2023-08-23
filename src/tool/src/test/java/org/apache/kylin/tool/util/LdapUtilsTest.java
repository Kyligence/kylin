/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.tool.util;

import java.util.HashSet;
import java.util.Set;

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LdapUtilsTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRewriteUserDn() {
        getTestConfig().setProperty("kylin.security.ldap.user-search-base", "cn=Users,dc=example,dc=com");
        String ldapUserSearchBase = getTestConfig().getLDAPUserSearchBase();
        Set<String> ldapUserDNs = new HashSet<>();
        ldapUserDNs.add("a");
        ldapUserDNs.add("b");
        ldapUserDNs = LdapUtils.rewriteUserDnIfNeeded(ldapUserDNs);
        Assert.assertTrue(ldapUserDNs.stream().allMatch(x -> x.contains(ldapUserSearchBase)));

        Set<String> ldapUserDNs2 = new HashSet<>();
        ldapUserDNs2.add("uid=a,cn=Users,dc=example,dc=com");
        ldapUserDNs2.add("uid=b,cn=Users,dc=example,dc=com");
        ldapUserDNs2 = LdapUtils.rewriteUserDnIfNeeded(ldapUserDNs2);
        Assert.assertTrue(ldapUserDNs2.stream().allMatch(x -> x.contains(ldapUserSearchBase)));
    }
}
