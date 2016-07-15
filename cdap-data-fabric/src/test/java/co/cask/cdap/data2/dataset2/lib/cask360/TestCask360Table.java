/*
 * Copyright © 2016 Cask Data, Inc.
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
package co.cask.cdap.data2.dataset2.lib.cask360;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Entity;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Tests for {@link Cask360Table}.
 */
public class TestCask360Table extends TestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @AfterClass
  public static void stopEverything() throws Exception {
    clear();
    finish();
  }

  @SuppressWarnings("rawtypes")
  public static class TestCask360App extends AbstractApplication {

    static final String TABLE_NAME = "test360table";

    @SuppressWarnings("unchecked")
    @Override
    public void configure() {
      setName("TestCask360App");
      createDataset(TABLE_NAME, Cask360Table.class);
    }
    
  }

  @Test
  public void test() throws Exception {
    // Deploy the Cask360Core application
    ApplicationManager appManager = deployApplication(TestCask360App.class);

    DataSetManager<Cask360Table> dataSetManager = getDataset(TestCask360App.TABLE_NAME);
    Cask360Table table = dataSetManager.get();

    // Lookup non-existing entity
    String id = "1";
    Cask360Entity entity = table.read(id);
    Assert.assertTrue("Expected null but query returned result",
        entity == null);

    // Write an entity directly to the table
    entity = TestCask360Entity.makeTestEntityOne(id);
    int id1count = entity.size();
    table.write(id, entity);
    dataSetManager.flush();

    // Read entity back directly from table
    Cask360Entity directEntity = table.read(id);
    Assert.assertTrue("Directly accessed entity different from written entity:\n" +
        entity.toString() + "\n" + directEntity.toString(),
        entity.equals(directEntity));
    
    // Now write twice to one entity and verify the merge

    // Lookup non-existing new entity
    id = "2";
    entity = table.read(id);
    Assert.assertTrue("Expected null but query returned result",
        entity == null);

    // Perform and verify first write to new entity
    Cask360Entity entityA = TestCask360Entity.makeTestEntityTwo(id);
    table.write(id, entityA);
    dataSetManager.flush();
    Cask360Entity directEntityA = table.read(id);
    Assert.assertTrue("Directly accessed entity different from written entity",
        entityA.equals(directEntityA));

    // Perform and verify second write to new entity is merged result
    Cask360Entity entityB = TestCask360Entity.makeTestEntityTwoOverlap(id);
    Cask360Entity combinedEntity = TestCask360Entity.makeTestEntityTwoCombined(id);
    table.write(id, entityB);
    dataSetManager.flush();
    Cask360Entity directEntityB = table.read(id);
    Assert.assertTrue("Directly accessed entity different from expected merged entity",
        combinedEntity.equals(directEntityB));
    int id2count = combinedEntity.size();
    int expectedCount = id1count + id2count;

    // 
    Connection conn = getQueryClient();
    Statement st = conn.createStatement();
    ResultSet res = st.executeQuery("SELECT * FROM dataset_" + TestCask360App.TABLE_NAME);
    int sqlCount = 0;
    while (res.next()) {
      sqlCount++;
    }
    Assert.assertEquals(expectedCount, sqlCount);
    res.close();
    st.close();
    conn.close();
    appManager.stopAll();
  }
}
