/*
 * Copyright © 2015-2016 Cask Data, Inc.
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
package co.cask.cdap.security.spi.authorization;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import co.cask.cdap.proto.security.Principal;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.EnumSet;

/**
 * Tests for {@link Authorizer}.
 */
public abstract class AuthorizerTest {

  private final NamespaceId namespace = Ids.namespace("foo");
  private final Principal user = new Principal("alice", Principal.PrincipalType.USER);

  protected abstract Authorizer get();

  @Test
  public void testSimple() throws UnauthorizedException {
    Authorizer authorizer = get();

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, Collections.singleton(Action.READ));
    authorizer.enforce(namespace, user, Action.READ);

    authorizer.revoke(namespace, user, Collections.singleton(Action.READ));
    verifyAuthFailure(namespace, user, Action.READ);
  }

  @Test
  public void testWildcard() throws UnauthorizedException {
    Authorizer authorizer = get();

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, EnumSet.allOf(Action.class));
    authorizer.enforce(namespace, user, Action.READ);
    authorizer.enforce(namespace, user, Action.WRITE);
    authorizer.enforce(namespace, user, Action.ADMIN);
    authorizer.enforce(namespace, user, Action.EXECUTE);

    authorizer.revoke(namespace, user, EnumSet.allOf(Action.class));
    verifyAuthFailure(namespace, user, Action.READ);
  }

  @Test
  public void testAll() throws UnauthorizedException {
    Authorizer authorizer = get();

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, Collections.singleton(Action.ALL));
    authorizer.enforce(namespace, user, Action.READ);
    authorizer.enforce(namespace, user, Action.WRITE);
    authorizer.enforce(namespace, user, Action.ADMIN);
    authorizer.enforce(namespace, user, Action.EXECUTE);

    authorizer.revoke(namespace, user, EnumSet.allOf(Action.class));
    verifyAuthFailure(namespace, user, Action.READ);

    Principal role = new Principal("admins", Principal.PrincipalType.ROLE);
    authorizer.grant(namespace, user, Collections.singleton(Action.READ));
    authorizer.grant(namespace, role, Collections.singleton(Action.ALL));
    authorizer.revoke(namespace);
    verifyAuthFailure(namespace, user, Action.READ);
    verifyAuthFailure(namespace, role, Action.ALL);
  }

  @Test
  public void testHierarchy() throws UnauthorizedException {
    Authorizer authorizer = get();

    DatasetId dataset = namespace.dataset("bar");

    verifyAuthFailure(namespace, user, Action.READ);

    authorizer.grant(namespace, user, Collections.singleton(Action.READ));
    authorizer.enforce(dataset, user, Action.READ);

    authorizer.grant(dataset, user, Collections.singleton(Action.WRITE));
    verifyAuthFailure(namespace, user, Action.WRITE);

    authorizer.revoke(namespace, user, Collections.singleton(Action.READ));
    verifyAuthFailure(namespace, user, Action.READ);
  }

  private void verifyAuthFailure(EntityId entity, Principal principal, Action action) {
    try {
      get().enforce(entity, principal, action);
      Assert.fail(String.format("Expected authorization failure, but it succeeded for entity %s, principal %s," +
                                  " action %s", entity, principal, action));
    } catch (UnauthorizedException expected) {
      // expected
    }
  }
}
