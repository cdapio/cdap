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

package io.cdap.cdap.proto.security;

import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.Ids;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

/**
 * Tests for {@link GrantRequest}.
 */
public class GrantRequestTest {

  @Test
  public void testValidation() {
    Principal bob = new Principal("bob", Principal.PrincipalType.USER);
    Set<Permission> permissions = Collections.singleton(StandardPermission.GET);
    new GrantRequest(Ids.namespace("foo"), bob, permissions);

    try {
      new GrantRequest(Ids.namespace("foo"), null, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest(Ids.namespace("foo"), bob, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest(Ids.namespace("foo"), null, permissions);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest((EntityId) null, bob, permissions);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest((EntityId) null, null, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
