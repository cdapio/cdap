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

package co.cask.cdap.data2.datafabric.dataset.service.mds;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.proto.id.DatasetModuleId;
import com.google.gson.Gson;
import com.google.gson.JsonParseException;
import org.junit.Assert;
import org.junit.Test;

public class BackwardCompatDatasetModuleIdDeserializerTest {

  private static DatasetTypeMDS mds = new DatasetTypeMDS(null, null);

  @Test
  public void testDeserialize40DatasetModuleId() throws Exception {
    // 4.0: {“module":"metricsTable-hbase","namespace":"system","entity":"DATASET_MODULE”}
    byte[] serialized = Bytes.toBytes(
      "{\"module\":\"metricsTable-hbase\",\"namespace\":\"system\",\"entity\":\"DATASET_MODULE\"}");
    DatasetModuleId moduleId = new DatasetModuleId("system", "metricsTable-hbase");
    Assert.assertEquals(moduleId,
                        mds.deserializeProxy(serialized, DatasetModuleId.class));
    Assert.assertEquals(moduleId,
                        mds.deserializeProxy(Bytes.toBytes(new Gson().toJson(moduleId)), DatasetModuleId.class));

  }

  @Test
  public void testDeserialize35IdDatasetModule() throws Exception {
    // 3.5: {"namespace":{"id":"system"},"moduleId":"metricsTable-hbase"}
    byte[] serialized = Bytes.toBytes("{\"namespace\":{\"id\":\"system\"},\"moduleId\":\"metricsTable-hbase\"}");
    Assert.assertEquals(new DatasetModuleId("system", "metricsTable-hbase"),
                        mds.deserializeProxy(serialized, DatasetModuleId.class));
  }

  @Test(expected = JsonParseException.class)
  public void testDeserializeInvalidInput() throws Exception {
    byte[] serialized = Bytes.toBytes("{\"namepace\":{\"id\":\"system\"},\"moleId\":\"metricsTable-hbase\"}");
    mds.deserializeProxy(serialized, DatasetModuleId.class);
  }
}
