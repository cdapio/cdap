/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.explore.table;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Tests for {@link AlterPartitionStatementBuilder}.
 */
public class AlterPartitionStatementBuilderTest {

  private final PartitionKey key = PartitionKey.builder().addIntField("year", 2012).build();
  private final String location = "/my/path";

  @Test
  public void test() {
    Assert.assertEquals("ALTER TABLE dbName.tblName ADD PARTITION (year=2012) LOCATION '/my/path'",
                        createStatementBuilder("dbName").buildAddStatement(location));
    Assert.assertEquals("ALTER TABLE dbName.tblName PARTITION (year=2012) CONCATENATE",
                        createStatementBuilder("dbName").buildConcatenateStatement());
    Assert.assertEquals("ALTER TABLE dbName.tblName DROP PARTITION (year=2012)",
                        createStatementBuilder("dbName").buildDropStatement());
  }

  @Test
  public void testWithNullDatabase() {
    Assert.assertEquals("ALTER TABLE tblName ADD PARTITION (year=2012) LOCATION '/my/path'",
                        createStatementBuilder(null).buildAddStatement(location));
    Assert.assertEquals("ALTER TABLE tblName PARTITION (year=2012) CONCATENATE",
                        createStatementBuilder(null).buildConcatenateStatement());
    Assert.assertEquals("ALTER TABLE tblName DROP PARTITION (year=2012)",
                        createStatementBuilder(null).buildDropStatement());

  }

  private AlterPartitionStatementBuilder createStatementBuilder(@Nullable String databaseName) {
    return new AlterPartitionStatementBuilder(databaseName, "tblName", key, false);
  }
}
