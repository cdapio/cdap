/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.common.metadata;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

import static io.cdap.cdap.api.metadata.MetadataScope.SYSTEM;
import static io.cdap.cdap.api.metadata.MetadataScope.USER;

public class CursorTest {

  @Test
  public void testToAndFromString() {
    Cursor[] cursors = {
      new Cursor(0, 100, true, SYSTEM, set("default"), set("program"), "name asc", "12345678", "*"),
      new Cursor(4, 8, false, null, set("default", "myns"), set("program", "dataset"), null, "12345678", "tags:*"),
      new Cursor(0, 0, false, null, null, null, null, "scroll|xyz", "schema:x:int"),
      new Cursor(1, 5, true, USER, set("system"), null, "", "a.b.c.d.e.f.g", "data*")
    };

    for (Cursor cursor : cursors) {
      Assert.assertEquals("for cursor: " + cursor, cursor, Cursor.fromString(cursor.toString()));
    }
  }

  private static Set<String> set(String ... strings) {
    return ImmutableSet.copyOf(strings);
  }
}
