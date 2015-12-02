/*
 * Copyright © 2015 Cask Data, Inc.
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
package co.cask.cdap.security.authorization;

import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.Ids;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.security.Action;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public abstract class AuthorizationPluginTest {

  protected abstract AuthorizationPlugin get();

  @Test
  public void testSimple() {
    AuthorizationPlugin handler = get();

    NamespaceId namespace = Ids.namespace("foo");
    String user = "alice";

    Assert.assertEquals(false, handler.authorized(namespace, user, ImmutableSet.of(Action.READ)));
    handler.grant(namespace, user, ImmutableSet.of(Action.READ));
    Assert.assertEquals(true, handler.authorized(namespace, user, ImmutableSet.of(Action.READ)));
    handler.revoke(namespace, user, ImmutableSet.of(Action.READ));
    Assert.assertEquals(false, handler.authorized(namespace, user, ImmutableSet.of(Action.READ)));
  }

  @Test
  public void testWildcard() {
    AuthorizationPlugin handler = get();

    NamespaceId namespace = Ids.namespace("foo");
    String user = "alice";

    Assert.assertEquals(false, handler.authorized(namespace, user, ImmutableSet.of(Action.READ)));
    handler.grant(namespace, user);
    Assert.assertEquals(true, handler.authorized(namespace, user, ImmutableSet.of(Action.READ, Action.WRITE)));
    handler.revoke(namespace, user);
    Assert.assertEquals(false, handler.authorized(namespace, user, ImmutableSet.of(Action.READ)));
  }

  @Test
  public void testAll() {
    AuthorizationPlugin handler = get();

    NamespaceId namespace = Ids.namespace("foo");
    String user = "alice";

    Assert.assertEquals(false, handler.authorized(namespace, user, ImmutableSet.of(Action.READ)));
    handler.grant(namespace, user, ImmutableSet.of(Action.ALL));
    Assert.assertEquals(true, handler.authorized(namespace, user, ImmutableSet.of(Action.READ, Action.WRITE)));
    handler.revoke(namespace, user, ImmutableSet.of(Action.ALL));
    Assert.assertEquals(false, handler.authorized(namespace, user, ImmutableSet.of(Action.READ)));
  }

  @Test
  public void testHierarchy() {
    AuthorizationPlugin handler = get();

    NamespaceId namespace = Ids.namespace("foo");
    DatasetId dataset = namespace.dataset("bar");
    String user = "alice";

    Assert.assertEquals(false, handler.authorized(dataset, user, ImmutableSet.of(Action.READ)));
    handler.grant(namespace, user, ImmutableSet.of(Action.READ));
    Assert.assertEquals(true, handler.authorized(dataset, user, ImmutableSet.of(Action.READ)));
    handler.revoke(namespace, user, ImmutableSet.of(Action.READ));
    Assert.assertEquals(false, handler.authorized(dataset, user, ImmutableSet.of(Action.READ)));
  }

}
