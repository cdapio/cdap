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
import co.cask.cdap.proto.ProgramRecord;
import co.cask.cdap.proto.ProgramType;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * StateStore Dataset Tests.
 */
public class StateStoreTableDatasetTest extends AbstractDatasetTest {

  @Test
  public void testBasics() throws Exception {
    addModule("stateStoreModule", new StateStoreTableModule());
    Map<String, String> content = Maps.newHashMap();
    content.put("k1", "v1");
    content.put("k2", "v2");
    content.put("key1", "val1");
    content.put("key2", "val2");

    createInstance(StateStoreTable.class.getName(), "myStateTable", DatasetProperties.EMPTY);
    StateStoreTable myStateTable = getInstance("myStateTable");

    ProgramRecord record = new ProgramRecord(ProgramType.FLOW, "MyApp", "MyFlow");
    Assert.assertEquals(null, myStateTable.getState(record));
    myStateTable.saveState(record, content);
    Assert.assertEquals(content.get("key1"), myStateTable.getState(record).get("key1"));
    Assert.assertEquals(false, myStateTable.getState(record).containsKey("key3"));
    Map<String, String> testContent = myStateTable.getState(record);
    Assert.assertEquals(content.size(), testContent.size());
    Assert.assertEquals(content.keySet(), testContent.keySet());
    content.remove("key1");
    myStateTable.saveState(record, content);
    Assert.assertEquals(content.size(), myStateTable.getState(record).size());
    Assert.assertEquals(false, myStateTable.getState(record).containsKey("key1"));
    deleteModule("stateStoreModule");
  }
}
