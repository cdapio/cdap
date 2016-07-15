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
package co.cask.cdap.cask360;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Entity;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Group;
import co.cask.cdap.api.dataset.lib.cask360.Cask360Table;
import co.cask.cdap.api.dataset.lib.cask360.Cask360GroupData.Cask360GroupDataMap;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.TestConfiguration;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Map;
import java.util.TreeMap;

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
    entity = makeTestEntityOne(id);
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
    Cask360Entity entityA = makeTestEntityTwo(id);
    table.write(id, entityA);
    dataSetManager.flush();
    Cask360Entity directEntityA = table.read(id);
    Assert.assertTrue("Directly accessed entity different from written entity",
        entityA.equals(directEntityA));

    // Perform and verify second write to new entity is merged result
    Cask360Entity entityB = makeTestEntityTwoOverlap(id);
    Cask360Entity combinedEntity = makeTestEntityTwoCombined(id);
    table.write(id, entityB);
    dataSetManager.flush();
    Cask360Entity directEntityB = table.read(id);
    Assert.assertTrue("Directly accessed entity different from expected merged entity",
        combinedEntity.equals(directEntityB));
    
    appManager.stopAll();
  }

  public static Cask360Entity makeTestEntityOne(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapOne = new TreeMap<String, String>();
    mapOne.put("a", "b");
    mapOne.put("c", "d");
    mapOne.put("e", "f");
    testData.put("one", new Cask360Group("one", new Cask360GroupDataMap(mapOne)));
    Map<String, String> mapTwo = new TreeMap<String, String>();
    mapTwo.put("g", "h");
    testData.put("two", new Cask360Group("two", new Cask360GroupDataMap(mapTwo)));
    Map<String, String> mapThree = new TreeMap<String, String>();
    mapThree.put("i", "j");
    mapThree.put("k", "l");
    testData.put("three", new Cask360Group("three", new Cask360GroupDataMap(mapThree)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTwo(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFour = new TreeMap<String, String>();
    mapFour.put("a", "b");
    testData.put("four", new Cask360Group("four", new Cask360GroupDataMap(mapFour)));
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "f");
    mapFive.put("g", "h");
    mapFive.put("i", "j");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "l");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapSeven = new TreeMap<String, String>();
    mapSeven.put("m", "n");
    mapSeven.put("o", "p");
    testData.put("seven", new Cask360Group("seven", new Cask360GroupDataMap(mapSeven)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTwoOverlap(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "F");
    mapFive.put("z", "z");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "L");
    mapSix.put("K", "k");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapEight = new TreeMap<String, String>();
    mapEight.put("m", "n");
    mapEight.put("o", "p");
    testData.put("eight", new Cask360Group("eight", new Cask360GroupDataMap(mapEight)));
    return new Cask360Entity(id, testData);
  }

  public static Cask360Entity makeTestEntityTwoCombined(String id) {
    Map<String, Cask360Group> testData = new TreeMap<String, Cask360Group>();
    Map<String, String> mapFour = new TreeMap<String, String>();
    mapFour.put("a", "b");
    testData.put("four", new Cask360Group("four", new Cask360GroupDataMap(mapFour)));
    Map<String, String> mapFive = new TreeMap<String, String>();
    mapFive.put("c", "d");
    mapFive.put("e", "F");
    mapFive.put("z", "z");
    mapFive.put("g", "h");
    mapFive.put("i", "j");
    testData.put("five", new Cask360Group("five", new Cask360GroupDataMap(mapFive)));
    Map<String, String> mapSix = new TreeMap<String, String>();
    mapSix.put("k", "L");
    mapSix.put("K", "k");
    testData.put("six", new Cask360Group("six", new Cask360GroupDataMap(mapSix)));
    Map<String, String> mapSeven = new TreeMap<String, String>();
    mapSeven.put("m", "n");
    mapSeven.put("o", "p");
    testData.put("seven", new Cask360Group("seven", new Cask360GroupDataMap(mapSeven)));
    Map<String, String> mapEight = new TreeMap<String, String>();
    mapEight.put("m", "n");
    mapEight.put("o", "p");
    testData.put("eight", new Cask360Group("eight", new Cask360GroupDataMap(mapEight)));
    return new Cask360Entity(id, testData);
  }
}
