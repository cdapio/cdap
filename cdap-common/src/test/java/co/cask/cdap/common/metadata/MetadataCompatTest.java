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

package co.cask.cdap.common.metadata;

import co.cask.cdap.api.metadata.Metadata;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.DatasetId;
import co.cask.cdap.proto.metadata.MetadataSearchResponse;
import co.cask.cdap.proto.metadata.MetadataSearchResponseV2;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecord;
import co.cask.cdap.proto.metadata.MetadataSearchResultRecordV2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;


/**
 * Tests for {@link MetadataCompat}
 */
public class MetadataCompatTest {

  @Test
  public void testMakeCompatibleMetadataRecordV2() {
    // should be compatible since the metadata entity represents a known cdap entity
    ImmutableMap<String, String> properties = ImmutableMap.of("k1", "v1");
    ImmutableSet<String> tags = ImmutableSet.of("t1");
    MetadataRecordV2 record =
      new MetadataRecordV2(MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
                             .appendAsType(MetadataEntity.APPLICATION, "app")
                             .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION).build(),
                           MetadataScope.USER, properties, tags);
    MetadataRecord expected = new MetadataRecord(new ApplicationId("ns", "app"), MetadataScope.USER, properties, tags);
    // should be compatible since the metadata entity represents a known cdap entity
    Optional<MetadataRecord> actual = MetadataCompat.makeCompatible(record);
    Assert.assertTrue(actual.isPresent());
    Assert.assertEquals(expected, actual.get());

    record = new MetadataRecordV2(MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
                                    .appendAsType(MetadataEntity.APPLICATION, "app")
                                    .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
                                    .appendAsType("something", "another").build(),
                                  MetadataScope.USER, properties, tags);

    actual = MetadataCompat.makeCompatible(record);
    Assert.assertFalse(actual.isPresent());
  }

  @Test
  public void testFilterForCompatibility() {
    ImmutableMap<String, String> properties = ImmutableMap.of("k1", "v1");
    ImmutableSet<String> tags = ImmutableSet.of("t1");
    MetadataRecordV2 compatibleRecord =
      new MetadataRecordV2(MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
                             .appendAsType(MetadataEntity.APPLICATION, "app")
                             .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
                             .build(),
                           MetadataScope.USER, properties, tags);
    MetadataRecordV2 incompatibleRecord =
      new MetadataRecordV2(MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "ns")
                             .appendAsType(MetadataEntity.APPLICATION, "app")
                             .append(MetadataEntity.VERSION, ApplicationId.DEFAULT_VERSION)
                             .appendAsType("something", "another").build(), MetadataScope.USER, properties, tags);
    MetadataRecord expected = new MetadataRecord(new ApplicationId("ns", "app"), MetadataScope.USER, properties, tags);
    Set<MetadataRecordV2> recordV2s = new HashSet<>(Arrays.asList(compatibleRecord, incompatibleRecord));
    // should only contain the compatible record which which belongs to the application
    Set<MetadataRecord> records = MetadataCompat.filterForCompatibility(recordV2s);
    Assert.assertEquals(1, records.size());
    Assert.assertEquals(expected, records.iterator().next());
  }

  @Test
  public void testMakeCompatibleMetadataSearchResultRecordV2() {
    Metadata systemMetadata = new Metadata(ImmutableMap.of("k1", "v1"), ImmutableSet.of("t1"));
    Metadata userMetadata = new Metadata(ImmutableMap.of("uk1", "uv1"), ImmutableSet.of("ut1"));
    MetadataSearchResultRecordV2 compatRecord =
      new MetadataSearchResultRecordV2(new DatasetId("ns", "ds").toMetadataEntity(),
                                       ImmutableMap.of(MetadataScope.SYSTEM, systemMetadata, MetadataScope.USER,
                                                       userMetadata));

    MetadataSearchResultRecordV2 incompatRecord =
      new MetadataSearchResultRecordV2(MetadataEntity.builder(new DatasetId("ns", "ds").toMetadataEntity())
                                         .appendAsType("field", "f1").build(),
                                       ImmutableMap.of(MetadataScope.SYSTEM, systemMetadata, MetadataScope.USER,
                                                       userMetadata));

    MetadataSearchResponseV2 metadataSearchResponseV2 =
      new MetadataSearchResponseV2("asc", 1, 1, 1, 1, new HashSet<>(Arrays.asList(incompatRecord, compatRecord)),
                                   new ArrayList<>(), true, new HashSet<>());

    MetadataSearchResultRecord expected =
      new MetadataSearchResultRecord(new DatasetId("ns", "ds"), ImmutableMap.of(MetadataScope.SYSTEM, systemMetadata,
                                                                                MetadataScope.USER,
                                                                                userMetadata));

    // should only contains the compatible record which belong to the dataset
    MetadataSearchResponse metadataSearchResponse = MetadataCompat.makeCompatible(metadataSearchResponseV2);
    Assert.assertEquals(1, metadataSearchResponse.getResults().size());
    Assert.assertEquals(expected, metadataSearchResponse.getResults().iterator().next());
  }
}
