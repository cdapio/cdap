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
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link AccessToken}.
 */
public class AccessTokenTest {

  @Test
  public void testToStringAndEquality() {
    UserIdentity identity = new UserIdentity(
        "alice", UserIdentity.IdentifierType.EXTERNAL, Arrays.asList("g1", "g2"), 1000L, 2000L);
    byte[] digest = new byte[]{'a', 'b', 0x01};
    AccessToken token = new AccessToken(identity, 7, digest);

    String expected = "AccessToken{identifier="
        + "UserIdentity{username=alice, tokenType=EXTERNAL, groups=[g1, g2], issueTimestamp=1000, expireTimestamp=2000}"
        + ", keyId=7, digest=ab\\x01}";
    Assert.assertEquals(expected, token.toString());

    AccessToken sameToken = new AccessToken(identity, 7, new byte[]{'a', 'b', 0x01});
    AccessToken differentKeyToken = new AccessToken(identity, 8, new byte[]{'a', 'b', 0x01});
    Assert.assertEquals(token, sameToken);
    Assert.assertEquals(token.hashCode(), sameToken.hashCode());
    Assert.assertNotEquals(token, differentKeyToken);
    Assert.assertSame(identity, token.getIdentifier());
    Assert.assertSame(identity, token.getMessage());
    Assert.assertEquals(7, token.getKeyId());
  }
}
