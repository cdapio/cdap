/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.data2.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link TableId}.
 */
public class TableIdTest {

  @Test
  public void testFromEqualsHashCodeAndExactToString() {
    TableId tableId = TableId.from("system", "app_meta");
    Assert.assertEquals("system", tableId.getNamespace());
    Assert.assertEquals("app_meta", tableId.getTableName());
    Assert.assertEquals("TableId{namespace=system, tableName=app_meta}", tableId.toString());

    TableId same = TableId.from("system", "app_meta");
    TableId different = TableId.from("default", "app_meta");
    Assert.assertEquals(tableId, same);
    Assert.assertEquals(tableId.hashCode(), same.hashCode());
    Assert.assertNotEquals(tableId, different);
    Assert.assertNotEquals(tableId, "not-a-table-id");
  }

  @Test
  public void testInvalidArgumentsRejected() {
    String[][] invalidPairs = new String[][]{
        {null, "tbl"},
        {"", "tbl"},
        {"ns", null},
        {"ns", ""}
    };
    for (String[] pair : invalidPairs) {
      try {
        TableId.from(pair[0], pair[1]);
        Assert.fail("Expected IllegalArgumentException");
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }
}
