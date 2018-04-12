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
import co.cask.cdap.data2.dataset2.lib.table.MDSKey;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.StreamId;
import co.cask.cdap.proto.id.StreamViewId;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link MetadataKey}
 */
public class MetadataKeyTest {
  @Test
  public void testGetMDSValueKey() {
    MDSKey mdsValueKey = MetadataKey.createValueRowKey(new ApplicationId("ns1", "app1").toMetadataEntity(), "key1");
    MDSKey.Splitter split = mdsValueKey.split();
    // skip value key bytes
    split.skipBytes();
    // assert target type is application
    Assert.assertEquals(MetadataEntity.APPLICATION, split.getString());

    // assert key-value pairs
    Assert.assertEquals(MetadataEntity.NAMESPACE, split.getString());
    Assert.assertEquals("ns1", split.getString());
    Assert.assertEquals(MetadataEntity.APPLICATION, split.getString());
    Assert.assertEquals("app1", split.getString());
    Assert.assertEquals(MetadataEntity.VERSION, split.getString());
    Assert.assertEquals(ApplicationId.DEFAULT_VERSION, split.getString());
    Assert.assertEquals("key1", split.getString());
    // asert that there is nothing more left in the key
    Assert.assertFalse(split.hasRemaining());
  }

  @Test
  public void testGetMetadataKey() {
    MDSKey mdsValueKey = MetadataKey.createValueRowKey(new ApplicationId("ns1", "app1").toMetadataEntity(), "key1");
    MDSKey mdsIndexKey = MetadataKey.createIndexRowKey(new ApplicationId("ns1", "app1").toMetadataEntity(),
                                                       "key1", "value1");

    // assert that metadata key for both index and value column is key1
    Assert.assertEquals("key1", MetadataKey.extractMetadataKey(mdsValueKey.getKey()));
    Assert.assertEquals("key1", MetadataKey.extractMetadataKey(mdsIndexKey.getKey()));
  }

  @Test
  public void testGetTargetType() {
    MDSKey mdsValueKey = MetadataKey.createValueRowKey(new ApplicationId("ns1", "app1").toMetadataEntity(), "key1");
    MDSKey mdsIndexKey = MetadataKey.createIndexRowKey(new ApplicationId("ns1", "app1").toMetadataEntity(),
                                                       "key1", "value1");

    // assert targetType
    Assert.assertEquals(MetadataEntity.APPLICATION, MetadataKey.extractTargetType(mdsValueKey.getKey()));
    Assert.assertEquals(MetadataEntity.APPLICATION, MetadataKey.extractTargetType(mdsIndexKey.getKey()));
  }

  @Test
  public void testGetTargetTypeChild() {
    StreamId expectedStreamId = new StreamId("ns1", "s1");
    MDSKey mdsValueKey = MetadataKey.createValueRowKey(expectedStreamId.toMetadataEntity(), "key1");
    StreamViewId expectedViewId = new StreamViewId("ns1", "s1", "v1");
    MDSKey mdsValueKey2 = MetadataKey.createValueRowKey(expectedViewId.toMetadataEntity(), "key2");

    // assert that the key for parent child are independent and correct
    MetadataEntity actualStreamId = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(expectedStreamId.toMetadataEntity(), actualStreamId);

    MetadataEntity actualViewId = MetadataKey.extractMetadataEntityFromKey(mdsValueKey2.getKey());
    Assert.assertEquals(expectedViewId.toMetadataEntity(), actualViewId);

    // assert that target type for parent child is correct
    Assert.assertEquals(MetadataEntity.STREAM, MetadataKey.extractTargetType(mdsValueKey.getKey()));
    Assert.assertEquals(MetadataEntity.VIEW, MetadataKey.extractTargetType(mdsValueKey2.getKey()));
  }

  @Test
  public void testGetMetadataEntityFromKey() {
    ApplicationId expectedAppId = new ApplicationId("ns1", "app1");
    MDSKey mdsValueKey = MetadataKey.createValueRowKey(expectedAppId.toMetadataEntity(), "key1");
    MDSKey mdsIndexKey = MetadataKey.createIndexRowKey(expectedAppId.toMetadataEntity(), "key1", "value1");

    // check that we can get MetadataEntity from value and index key
    MetadataEntity actualAppId = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(expectedAppId.toMetadataEntity(), actualAppId);
    actualAppId = MetadataKey.extractMetadataEntityFromKey(mdsIndexKey.getKey());
    Assert.assertEquals(expectedAppId.toMetadataEntity(), actualAppId);
  }

  @Test
  public void testGetMDSIndexKey() {
    MDSKey mdsIndexKey = MetadataKey.createIndexRowKey(new ApplicationId("ns1", "app1").toMetadataEntity(),
                                                       "key1", "value1");
    MDSKey.Splitter split = mdsIndexKey.split();
    // skip value key bytes
    split.skipBytes();
    // assert target type
    Assert.assertEquals(MetadataEntity.APPLICATION, split.getString());

    // assert key-valu pairs
    Assert.assertEquals(MetadataEntity.NAMESPACE, split.getString());
    Assert.assertEquals("ns1", split.getString());
    Assert.assertEquals(MetadataEntity.APPLICATION, split.getString());
    Assert.assertEquals("app1", split.getString());
    Assert.assertEquals(MetadataEntity.VERSION, split.getString());
    Assert.assertEquals(ApplicationId.DEFAULT_VERSION, split.getString());
    Assert.assertEquals("key1", split.getString());
    Assert.assertEquals("value1", split.getString());
    Assert.assertFalse(split.hasRemaining());
  }
}
