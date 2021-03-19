/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.cdap.security.spi.authorization;

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.security.Action;
import io.cdap.cdap.proto.security.Principal;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

public class UnauthorizedExceptionTest {
  private static final Principal TEST_PRINCIPAL = new Principal("test-principal", Principal.PrincipalType.USER);
  private static final EntityId TEST_NAMESPACE_ENTITY = new NamespaceId("test");

  @Test
  public void testSingleActionReturnsExpectedMessage() {
    String expected = String.format("Principal '%s' is not authorized to perform action '%s' on entity '%s'",
                                    TEST_PRINCIPAL, Action.ADMIN, TEST_NAMESPACE_ENTITY);
    String got = new UnauthorizedException(TEST_PRINCIPAL, Action.ADMIN, TEST_NAMESPACE_ENTITY).getMessage();
    Assert.assertEquals(expected, got);
  }

  @Test
  public void testNoActionsReturnsExpectedMessage() {
    String expected = String.format("Principal '%s' is not authorized to access entity '%s'",
                                    TEST_PRINCIPAL, TEST_NAMESPACE_ENTITY);
    String got = new UnauthorizedException(TEST_PRINCIPAL, Collections.emptySet(), TEST_NAMESPACE_ENTITY).getMessage();
    Assert.assertEquals(expected, got);
  }

  @Test
  public void testMultipleActionsReturnsExpectedMessage() {
    Set<Action> actions = new LinkedHashSet<>();
    actions.add(Action.ADMIN);
    actions.add(Action.EXECUTE);
    String expected = String.format("Principal '%s' is not authorized to perform actions '%s' on entity '%s'",
                                    TEST_PRINCIPAL, actions, TEST_NAMESPACE_ENTITY);
    String got = new UnauthorizedException(TEST_PRINCIPAL, actions, TEST_NAMESPACE_ENTITY).getMessage();
    Assert.assertEquals(expected, got);
  }

  @Test
  public void testMultipleActionsWithThrowableReturnsExpectedCause() {
    Exception e = new Exception("test");
    Set<Action> actions = new LinkedHashSet<>();
    actions.add(Action.ADMIN);
    actions.add(Action.EXECUTE);
    UnauthorizedException unauthorizedException = new UnauthorizedException(TEST_PRINCIPAL, actions,
                                                                            TEST_NAMESPACE_ENTITY, e);
    Assert.assertEquals(e, unauthorizedException.getCause());
  }

  @Test
  public void testMultipleActionsWithoutMustHaveAllReturnsExpectedMessage() {
    Set<Action> actions = new LinkedHashSet<>();
    actions.add(Action.ADMIN);
    actions.add(Action.EXECUTE);
    String expected = String.format("Principal '%s' is not authorized to perform any one of the actions '%s' " +
                                      "on entity '%s'", TEST_PRINCIPAL, actions, TEST_NAMESPACE_ENTITY);
    String got = new UnauthorizedException(TEST_PRINCIPAL, actions, TEST_NAMESPACE_ENTITY, false).getMessage();
    Assert.assertEquals(expected, got);
  }

  @Test
  public void testOneActionWithoutMustHaveAllReturnsExpectedMessage() {
    Set<Action> actions = new LinkedHashSet<>();
    actions.add(Action.ADMIN);
    String expected = String.format("Principal '%s' is not authorized to perform action '%s' on entity '%s'",
                                    TEST_PRINCIPAL, Action.ADMIN, TEST_NAMESPACE_ENTITY);
    String got = new UnauthorizedException(TEST_PRINCIPAL, actions, TEST_NAMESPACE_ENTITY, false).getMessage();
    Assert.assertEquals(expected, got);
  }

  @Test
  public void testOneActionWithoutPrincipalReturnsExpectedMessage() {
    Set<String> actions = new LinkedHashSet<>();
    actions.add(Action.ADMIN.toString());
    String entityString = String.format("entity '%s'", TEST_NAMESPACE_ENTITY);
    String expected = String.format("You are not authorized to perform action '%s' on %s", Action.ADMIN,
                                    entityString);
    String got = new UnauthorizedException(null, actions, entityString, null, true, false, null).getMessage();
    Assert.assertEquals(expected, got);
  }

  @Test
  public void testCustomEntityWithCustomPermissionsReturnsExpectedMessage() {
    Set<String> permissions = new LinkedHashSet<>();
    permissions.add("test-permission-1");
    permissions.add("test-permissions-2");
    String entityString = "custom resource 'test-resource'";
    String expected = String.format("You are not authorized to perform actions '%s' on %s", permissions,
                                    entityString);
    String got = new UnauthorizedException(null, permissions, entityString, null, true, false, null).getMessage();
    Assert.assertEquals(expected, got);
  }

  @Test
  public void testCustomAddendumReturnsExpectedMessage() {
    Set<String> permissions = new LinkedHashSet<>();
    permissions.add("test-permission-1");
    permissions.add("test-permissions-2");
    String entityString = "custom resource 'test-resource'";
    String expected = String.format("You are not authorized to perform actions '%s' on %s test addendum", permissions,
                                    entityString);
    String got = new UnauthorizedException(null, permissions, entityString, null, true, false, " test addendum")
      .getMessage();
    Assert.assertEquals(expected, got);
  }
}
