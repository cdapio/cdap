/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.proto.id;

import co.cask.cdap.api.metadata.MetadataEntity;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test fro {@link CustomEntityId}
 */
public class CustomEntityIdTest {

  @Test
  public void test() {
    DatasetId datasetId = new DatasetId("myNs", "myDs");
    MetadataEntity metadataEntity = datasetId.toMetadataEntity()
      .append("k1", "v1").append("k2", "v2");
    CustomEntityId customEntityId = new CustomEntityId(datasetId, metadataEntity);
    Assert.assertEquals("v2", customEntityId.getEntityName());
    Assert.assertEquals("k2", customEntityId.getCustomType());
    Assert.assertEquals(ImmutableList.of(new MetadataEntity.KeyValue("k1", "v1"),
                                         new MetadataEntity.KeyValue("k2", "v2")), customEntityId.getSubParts());

    try {
      customEntityId.toIdParts();
      Assert.fail("Should have failed to call toIdParts");
    } catch (UnsupportedOperationException e) {
      // expected
    }
  }
}
