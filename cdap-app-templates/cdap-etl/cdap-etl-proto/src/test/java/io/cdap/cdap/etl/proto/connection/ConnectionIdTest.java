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
 *
 */

package io.cdap.cdap.etl.proto.connection;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test connection id generation
 */
public class ConnectionIdTest {

  @Test
  public void testConnectionIdGeneration() throws Exception {
    Assert.assertEquals("a_b_c", ConnectionId.getConnectionId("a.b.c"));
    Assert.assertEquals("a_b_c", ConnectionId.getConnectionId("a^b^c"));
    Assert.assertEquals("a_b_c", ConnectionId.getConnectionId("a b c"));
    Assert.assertEquals("A_B_c", ConnectionId.getConnectionId("A%B%c"));
    Assert.assertEquals("A__B__c", ConnectionId.getConnectionId("A%.B%#c"));
    Assert.assertEquals("__A__B__c", ConnectionId.getConnectionId("&&A%.B%#c"));
    Assert.assertEquals("__A__B__c", ConnectionId.getConnectionId("  &&A%.B%#c  "));

    // test long string get truncated
    StringBuilder longstring = new StringBuilder();
    StringBuilder expected = new StringBuilder();
    for (int i = 0; i < 100; i++) {
      if (i % 2 == 0) {
        expected.append("a");
      }
      longstring.append("a");
    }
    Assert.assertEquals(expected.toString(), ConnectionId.getConnectionId(longstring.toString()));

    assertInvalidConnectionName("$");
    assertInvalidConnectionName("(");
    assertInvalidConnectionName(")");
    assertInvalidConnectionName("{");
    assertInvalidConnectionName("}");
    assertInvalidConnectionName("${abc}");
    assertInvalidConnectionName("${a(b)}");
    assertInvalidConnectionName("$abcdedfg");
  }

  public void assertInvalidConnectionName(String name) {
    try {
      ConnectionId.getConnectionId(name);
      Assert.fail();
    } catch (ConnectionBadRequestException e) {
      // expected
    }
  }
}
