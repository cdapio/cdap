/*
 * Copyright 2018 Cask Data, Inc.
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
package co.cask.cdap.data2.metadata.dataset;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.id.NamespaceId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link Metadata}
 */
public class MetadataTest {
  @Test
  public void testToEntityId() {
    // should be able to get get an EntityId if Metadata belong to a cdap entity id
    DatasetId myDs = NamespaceId.DEFAULT.dataset("myDs");
    Metadata metadata1 = new Metadata(myDs);
    Assert.assertEquals(myDs, metadata1.getEntityId());

    MetadataEntity metadataEntity = MetadataEntity.ofDataset(NamespaceId.DEFAULT.getEntityName(), "myDs")
      .append("field", "myField");
    Metadata metadata2 = new Metadata(metadataEntity);
    try {
      metadata2.getEntityId();
      Assert.fail();
    } catch (IllegalArgumentException e) {
      // expected
    }
  }
}
