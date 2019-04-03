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

package co.cask.cdap.replication;

import co.cask.cdap.api.common.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.UUID;

/**
 * Test cases for ReplicationStatusKey
 */
public class ReplicationStatusKeyTest {

  @Test
  public void testBasic() {
    String rowType = "RowType";
    String regionName = "RegionName";
    UUID regionServerID = UUID.randomUUID();
    ReplicationStatusKey key = new ReplicationStatusKey(rowType, regionName, regionServerID);

    ReplicationStatusKey keyTest = new ReplicationStatusKey(key.getKey());
    Assert.assertEquals(keyTest.getRowType(), rowType);
    Assert.assertEquals(keyTest.getRegionName(), regionName);
    Assert.assertEquals(keyTest.getRsID(), regionServerID);
  }
}
