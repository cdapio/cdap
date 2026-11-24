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

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link KeyIdentifier}.
 */
public class KeyIdentifierTest {

  @Test
  public void testToStringAndEquality() {
    SecretKey secretKey = new SecretKeySpec(new byte[]{1, 2, 3, 4}, "HmacSHA1");
    KeyIdentifier keyIdentifier = new KeyIdentifier(secretKey, 42, 987654321L);

    Assert.assertEquals("KeyIdentifier{keyId=42, expiration=987654321}", keyIdentifier.toString());
    Assert.assertEquals(42, keyIdentifier.getKeyId());
    Assert.assertEquals(987654321L, keyIdentifier.getExpiration());
    Assert.assertEquals(secretKey, keyIdentifier.getKey());
    Assert.assertEquals(keyIdentifier.hashCode(), keyIdentifier.hashCode());

    KeyIdentifier same = new KeyIdentifier(new SecretKeySpec(new byte[]{1, 2, 3, 4}, "HmacSHA1"), 42, 987654321L);
    KeyIdentifier differentId = new KeyIdentifier(secretKey, 43, 987654321L);
    Assert.assertEquals(keyIdentifier, same);
    Assert.assertNotEquals(keyIdentifier, differentId);
  }
}
