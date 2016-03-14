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

package co.cask.cdap.proto.security;

import co.cask.cdap.proto.id.Ids;

import org.junit.Assert;
import org.junit.Test;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Tests for {@link CheckAuthorizedRequest}.
 */
public class CheckAuthorizedRequestTest {

  @Test
  public void testValidation() {
    Principal bob = new Principal("bob", Principal.PrincipalType.USER);
    Set<Action> actions = new LinkedHashSet<>();
    actions.add(Action.READ);
    new CheckAuthorizedRequest(Ids.namespace("foo"), bob, actions);

    try {
      new CheckAuthorizedRequest(Ids.namespace("foo"), null, null);
      Assert.fail("Expected IllegalArgument");
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new CheckAuthorizedRequest(Ids.namespace("foo"), bob, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new CheckAuthorizedRequest(Ids.namespace("foo"), null, actions);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new CheckAuthorizedRequest(null, bob, actions);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new CheckAuthorizedRequest(null, null, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
