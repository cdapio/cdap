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

package co.cask.cdap.proto.security;

import co.cask.cdap.proto.id.Ids;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class GrantRequestTest {

  @Test
  public void testValidation() {
    new GrantRequest(Ids.namespace("foo"), "bob", ImmutableSet.of(Action.READ));

    try {
      new GrantRequest(Ids.namespace("foo"), null, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest(Ids.namespace("foo"), "bob", null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest(Ids.namespace("foo"), null, ImmutableSet.of(Action.READ));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest(null, "bob", ImmutableSet.of(Action.READ));
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }

    try {
      new GrantRequest(null, null, null);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
