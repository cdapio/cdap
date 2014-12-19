/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.data2.dataset2.AbstractDatasetTest;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

/**
 * StateStore Dataset Tests.
 */
public class StateStoreTableDatasetTest extends AbstractDatasetTest {
  private static final String MODULE_NAME = "stateStoreModule";
  private static final String INSTANCE_NAME = "myStateTable";
  private static StateStoreTable myStateTable;

  @BeforeClass
  public static void beforeClass() throws Exception {
    addModule(MODULE_NAME, new StateStoreTableModule());
    createInstance(StateStoreTable.class.getName(), INSTANCE_NAME, DatasetProperties.EMPTY);
    myStateTable = getInstance(INSTANCE_NAME);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    myStateTable.close();
    deleteInstance(INSTANCE_NAME);
    deleteModule(MODULE_NAME);
  }

  @Test
  public void testBasics() throws Exception {
    Map<String, String> content = Maps.newHashMap();
    content.put("k1", "v1");
    content.put("k2", "v2");
    content.put("key1", "val1");
    content.put("key2", "val2");

    ProgramRecord record = new ProgramRecord(ProgramType.FLOW, "MyApp", "MyFlow");
    Assert.assertEquals(0, myStateTable.getState(record).size());
    myStateTable.saveState(record, content);
    Assert.assertEquals(content, myStateTable.getState(record));

    content.remove("key1");
    myStateTable.saveState(record, content);
    Assert.assertEquals(content, myStateTable.getState(record));

    myStateTable.deleteState(record);
    Assert.assertEquals(0, myStateTable.getState(record).size());
  }

  @Test
  public void testMultiPrograms() throws Exception {
    ProgramRecord record1 = new ProgramRecord(ProgramType.FLOW, "MyApp", "MyFlow1");
    ProgramRecord record2 = new ProgramRecord(ProgramType.FLOW, "MyApp", "MyFlow2");
    ProgramRecord record3 = new ProgramRecord(ProgramType.FLOW, "MyApp", "MyFlow3");
    ProgramRecord record4 = new ProgramRecord(ProgramType.FLOW, "NotMyApp", "MyFlow");
    Map<String, String> content = ImmutableMap.of("key", "value");
    Id.Application appId = new Id.Application(new Id.Account("myId"), "MyApp");

    myStateTable.saveState(record1, content);
    myStateTable.saveState(record2, content);
    myStateTable.saveState(record3, content);
    myStateTable.saveState(record4, content);

    Assert.assertEquals(content, myStateTable.getState(record1));
    Assert.assertEquals(content, myStateTable.getState(record4));

    myStateTable.deleteState(record1);
    Assert.assertEquals(content, myStateTable.getState(record2));
    Assert.assertEquals(content, myStateTable.getState(record3));

    myStateTable.deleteState(appId);
    Assert.assertEquals(0, myStateTable.getState(record1).size());
    Assert.assertEquals(0, myStateTable.getState(record2).size());
    Assert.assertEquals(0, myStateTable.getState(record3).size());
    Assert.assertEquals(content, myStateTable.getState(record4));

    myStateTable.deleteState(record4);
    Assert.assertEquals(0, myStateTable.getState(record4).size());
  }
}
