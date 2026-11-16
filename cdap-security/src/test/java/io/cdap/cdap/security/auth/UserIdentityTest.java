/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.security.auth;

import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link UserIdentity}.
 */
public class UserIdentityTest {

  @Test
  public void testToStringAndIdentifierTypeFallback() {
    UserIdentity identity = new UserIdentity(
        "bob", UserIdentity.IdentifierType.INTERNAL, Arrays.asList("eng", "ops"), 100L, 500L);
    Assert.assertEquals(
        "UserIdentity{username=bob, tokenType=INTERNAL, groups=[eng, ops], issueTimestamp=100, expireTimestamp=500}",
        identity.toString());
    Assert.assertEquals(UserIdentity.IdentifierType.INTERNAL, identity.getIdentifierType());

    UserIdentity nullTypeIdentity = new UserIdentity("bob", null, Collections.emptyList(), 100L, 500L);
    Assert.assertEquals(
        "UserIdentity{username=bob, tokenType=null, groups=[], issueTimestamp=100, expireTimestamp=500}",
        nullTypeIdentity.toString());
    Assert.assertEquals(UserIdentity.IdentifierType.INVALID, nullTypeIdentity.getIdentifierType());
  }
}
