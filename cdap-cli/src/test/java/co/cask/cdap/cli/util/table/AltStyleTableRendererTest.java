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

import com.google.common.base.Strings;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;

/**
 *
 */
@Ignore
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class AltStyleTableRendererTest extends TableRendererTest {

  @Override
  public TableRenderer getRenderer() {
    return new AltStyleTableRenderer();
  }

  @Test
  public void testOneCharLongerThanWidth() {
    Table table = Table.builder()
      .setHeader("c1")
      .setRows(Table.rows()
                 .add(Strings.repeat("z", (LINE_WIDTH - 4)) + "a")
                 .build())
      .build();
    getRenderer().render(TEST_CONFIG, OUTPUT, table);
  }

  @Test
  public void testHeaderOneCharLongerThanWidth() {
    Table table = Table.builder()
      .setHeader(Strings.repeat("z", (LINE_WIDTH - 4)) + "a")
      .setRows(Table.rows().add("abc").build())
      .build();
    getRenderer().render(TEST_CONFIG, OUTPUT, table);
  }

  @Test
  public void testOneCharLongerThan2Width() {
    Table table = Table.builder()
      .setHeader("c1")
      .setRows(Table.rows().add(Strings.repeat("z", (LINE_WIDTH - 4) * 2) + "a").build())
      .build();
    getRenderer().render(TEST_CONFIG, OUTPUT, table);
  }

}
