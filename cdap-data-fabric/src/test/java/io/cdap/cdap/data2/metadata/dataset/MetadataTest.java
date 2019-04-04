/*
 * Copyright 2018-2019 Cask Data, Inc.
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
package io.cdap.cdap.data2.metadata.dataset;

import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link MetadataDataset.Record}
 */
public class MetadataTest {
  @Test
  public void testToEntityId() {
    // should be able to get get an EntityId if Metadata belong to a cdap entity id
    DatasetId myDs = NamespaceId.DEFAULT.dataset("myDs");
    MetadataDataset.Record metadata1 = new MetadataDataset.Record(myDs);
    Assert.assertEquals(myDs, metadata1.getEntityId());

    MetadataEntity metadataEntity =
      MetadataEntity.builder(MetadataEntity.ofDataset(NamespaceId.DEFAULT.getEntityName(), "myDs"))
        .appendAsType("field", "myField").build();
    MetadataDataset.Record metadata2 = new MetadataDataset.Record(metadataEntity);
    try {
      metadata2.getEntityId();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
