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
package co.cask.cdap.cli.util.table;

import com.google.common.collect.ImmutableList;
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
@Ignore
public abstract class TableRendererTest {

  public abstract TableRenderer getRenderer();

  @Test
  public void testFormat() {
    Table table = Table.builder()
      .setHeader("c1", "c2", "c3333")
      .setRows(ImmutableList.of(
        new String[]{"r1\n456", "r11", "r1"},
        new String[]{"r2", "r2222\n123", "r"},
        new String[]{"r3333", "r3", "r3\n1"}))
      .build();
    getRenderer().render(System.out, table);
  }

}
