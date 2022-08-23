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

package org.apache.kylin.rest.service;

import static org.apache.kylin.rest.constant.Constant.ROLE_ADMIN;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.metadata.user.ManagedUser;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.security.AclPermission;
import org.apache.kylin.rest.security.ExternalAclProvider;
import org.apache.kylin.rest.security.UserAclManager;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.test.util.ReflectionTestUtils;

public class UserAclServiceTest extends ServiceTestBase {

    @Mock
    protected UserAclService userAclService = Mockito.spy(new UserAclService());

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        super.setup();
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
        ReflectionTestUtils.setField(userAclService, "userService", userService);
    }

    @Test
    public void testCreateUser() {
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        if (!userService.userExists("ADMIN1")) {
            userService.createUser(new ManagedUser("ADMIN1", "ADMIN1", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.grantUserAclPermission("ADMIN1", "DATA_QUERY");
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        Assert.assertEquals(2, userAclService.listUsersHasGlobalPermission("DATA_QUERY").size());
        userAclService.revokeUserAclPermission("ADMIN1", "DATA_QUERY");
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));

        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getModifyPermissionOfSuperAdminFailed());
        userAclService.grantUserAclPermission("admin", "DATA_QUERY");

    }

    @Test
    public void testGetAllUsersHasGlobalPermission() {
        KylinUserService kylinUserService = new KylinUserService() {
            @Override
            public List<String> listAdminUsers() throws IOException {
                throw new IOException("test");
            }
        };
        ReflectionTestUtils.setField(userAclService, "userService", kylinUserService);
        Assert.assertTrue(userAclService.listUsersHasGlobalPermission(ExternalAclProvider.DATA_QUERY).isEmpty());
        ReflectionTestUtils.setField(userAclService, "userService", SpringContext.getBean(UserService.class));
    }

    @Test
    public void testGrantUserAclExceptions() {
        Assert.assertThrows(KylinException.class, () -> userAclService.grantUserAclPermission("ADMIN", "DATA_QUERY"));
    }

    @Test
    public void testRevokeUserAclExceptions() {
        Assert.assertThrows(KylinException.class, () -> userAclService.revokeUserAclPermission("ADMIN", "DATA_QUERY"));
    }

    @Test
    public void testCheckAclPermission() {
        Assert.assertThrows(IllegalArgumentException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "", ""));
        Assert.assertThrows(MsgPicker.getMsg().getModifyPermissionOfSuperAdminFailed(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "admin", "DATA_QUERY"));

        ReflectionTestUtils.setField(userAclService, "userService", new KylinUserService() {
            public List<String> listSuperAdminUsers() {
                return Collections.emptyList();
            }
        });
        Assert.assertThrows(MsgPicker.getMsg().getModifyOwnPermissionFailed(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "admin", "DATA_QUERY"));
        UserAclManager manager = userAclService.getManager(UserAclManager.class);
        manager.deletePermission("admin", AclPermission.DATA_QUERY);
        Assert.assertThrows(MsgPicker.getMsg().getGrantPermissionFailedByIllegalAuthorizingUser(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "admin", "DATA_QUERY"));
        Assert.assertTrue(userAclService.listUsersHasGlobalPermission("DATA_QUERY").isEmpty());
        manager.addPermission("admin", AclPermission.DATA_QUERY);
        Assert.assertThrows(MsgPicker.getMsg().getGrantPermissionFailedByNonSystemAdmin(), KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(userAclService, "checkAclPermission", "test", "DATA_QUERY"));
        ReflectionTestUtils.setField(userAclService, "userService", userService);
    }

    @Test
    public void testCheckAdminUser() {
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getEmptySid());
        ReflectionTestUtils.invokeMethod(userAclService, "checkAdminUser", "");
        thrown.expect(KylinException.class);
        thrown.expectMessage(
                String.format(Locale.ROOT, MsgPicker.getMsg().getOperationFailedByUserNotExist(), "test_not"));
        ReflectionTestUtils.invokeMethod(userAclService, "checkAdminUser", "test_not");
        thrown.expect(KylinException.class);
        thrown.expectMessage(MsgPicker.getMsg().getGrantPermissionFailedByNonSystemAdmin());
        ReflectionTestUtils.invokeMethod(userAclService, "checkAdminUser", "test");
    }

    @Test
    public void testUpdateGlobalPermission() {
        if (!userService.userExists("ADMIN1")) {
            userService.createUser(new ManagedUser("ADMIN1", "ADMIN1", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.grantUserAclPermission("ADMIN1", "DATA_QUERY");
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        UserDetails userDetails = userService.loadUserByUsername("ADMIN1");
        userDetails.getAuthorities().remove(new SimpleGrantedAuthority(ROLE_ADMIN));
        userService.updateUser(userDetails);
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN1", AclPermission.DATA_QUERY));
        Assert.assertFalse(userAclService.getManager(UserAclManager.class).exists("admin1"));

        if (!userService.userExists("ADMIN2")) {
            userService.createUser(new ManagedUser("ADMIN2", "ADMIN2", false,
                    Arrays.asList(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.updateUserAclPermission(userService.loadUserByUsername("ADMIN2"), AclPermission.DATA_QUERY);
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN2", AclPermission.DATA_QUERY));

        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        if (!userService.userExists("ADMIN3")) {
            userService.createUser(new ManagedUser("ADMIN3", "ADMIN3", false,
                    Arrays.asList(new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        userAclService.updateUserAclPermission(userService.loadUserByUsername("ADMIN3"), AclPermission.DATA_QUERY);
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN3", AclPermission.DATA_QUERY));
    }

    @Test
    public void testDeleteUser() {
        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "false");
        if (!userService.userExists("ADMIN4")) {
            userService.createUser(new ManagedUser("ADMIN4", "ADMIN4", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        Assert.assertFalse(userAclService.hasUserAclPermission("ADMIN4", AclPermission.DATA_QUERY));
        userService.deleteUser("ADMIN4");
        Assert.assertFalse(userAclService.getManager(UserAclManager.class).exists("ADMIN4"));

        getTestConfig().setProperty("kylin.security.acl.data-permission-default-enabled", "true");
        if (!userService.userExists("ADMIN4")) {
            userService.createUser(new ManagedUser("ADMIN4", "ADMIN4", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN))));
        }
        Assert.assertTrue(userAclService.hasUserAclPermission("ADMIN4", AclPermission.DATA_QUERY));
        userService.deleteUser("ADMIN4");
        Assert.assertFalse(userAclService.getManager(UserAclManager.class).exists("ADMIN4"));
    }

    @Test
    public void testSyncAdminUserAcl() {
        userAclService.syncAdminUserAcl();
        Assert.assertTrue(userAclService.hasUserAclPermission("admin", AclPermission.DATA_QUERY));
    }

    @Test
    public void testSuperAdmin() {
        Assert.assertTrue(userAclService.isSuperAdmin());
        Assert.assertTrue(userAclService.canAdminUserQuery());
        Mockito.when(userAclService.isSuperAdmin()).thenReturn(false);
        Assert.assertTrue(userAclService.canAdminUserQuery());
    }
}
