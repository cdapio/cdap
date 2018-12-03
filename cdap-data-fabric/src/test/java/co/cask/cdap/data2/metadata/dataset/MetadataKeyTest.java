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
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.proto.id.ScheduleId;
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
  public void testVersionedEntitiesKey() {
    // CDAP-13597 Metadata for versioned entity is version independent i.e if there are two application version v1
    // and v2 and a tag 'tag1' is added to either one it will be be reflected to both as we don't store the
    // application/schedule/programs with it's version. Following tests test that for such versioned entity the keys
    // are the same i.e default version
    // Key for versioned application/schedule/program should be the same
    // application
    ApplicationId applicationId1 = new ApplicationId("ns", "app"); // default version
    ApplicationId applicationId2 = new ApplicationId("ns", "app", "2"); // custom version
    // non-version Application metadata entity

    MDSKey mdsValueKey = MetadataKey.createValueRowKey(applicationId1.toMetadataEntity(), "key1");
    MetadataEntity actual = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(applicationId1.toMetadataEntity(), actual);
    mdsValueKey = MetadataKey.createValueRowKey(applicationId2.toMetadataEntity(), "key1");
    actual = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(applicationId1.toMetadataEntity(), actual);

    // program
    ProgramId programId1 = new ApplicationId("ns", "app").program(ProgramType.FLOW, "f"); // default version
    ProgramId programId2 = new ApplicationId("ns", "app", "2").program(ProgramType.FLOW, "f"); // custom version

    mdsValueKey = MetadataKey.createValueRowKey(programId1.toMetadataEntity(), "key1");
    actual = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(programId1.toMetadataEntity(), actual);
    mdsValueKey = MetadataKey.createValueRowKey(programId2.toMetadataEntity(), "key1");
    actual = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(programId1.toMetadataEntity(), actual);

    // schedule
    ScheduleId scheduleId1 = new ApplicationId("ns", "app").schedule("s"); // default version
    ScheduleId scheduleId2 = new ApplicationId("ns", "app", "2").schedule("s"); // custom version

    mdsValueKey = MetadataKey.createValueRowKey(scheduleId1.toMetadataEntity(), "key1");
    actual = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(scheduleId1.toMetadataEntity(), actual);
    mdsValueKey = MetadataKey.createValueRowKey(scheduleId2.toMetadataEntity(), "key1");
    actual = MetadataKey.extractMetadataEntityFromKey(mdsValueKey.getKey());
    Assert.assertEquals(scheduleId1.toMetadataEntity(), actual);
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

    // assert key-value pairs
    Assert.assertEquals(MetadataEntity.NAMESPACE, split.getString());
    Assert.assertEquals("ns1", split.getString());
    Assert.assertEquals(MetadataEntity.APPLICATION, split.getString());
    Assert.assertEquals("app1", split.getString());
    Assert.assertEquals("key1", split.getString());
    Assert.assertEquals("value1", split.getString());
    Assert.assertFalse(split.hasRemaining());
  }
}
