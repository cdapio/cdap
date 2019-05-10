/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.metadata;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.spi.metadata.Metadata;
import io.cdap.cdap.spi.metadata.MetadataDirective;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;

public class DefaultMetadataServiceClientTest extends AppFabricTestBase {
  // - keep description if new metadata does not contain it
  // - preserve creation-time if it exists in current metadata
  private static final Map<ScopedNameOfKind, MetadataDirective> CREATE_DIRECTIVES = ImmutableMap.of(
    new ScopedNameOfKind(MetadataKind.TAG, MetadataScope.USER, "c"),
    MetadataDirective.KEEP,
    new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.USER, "z"),
    MetadataDirective.PRESERVE);

  private final Metadata testMetadata = new Metadata(
    ImmutableSet.of(new ScopedName(MetadataScope.SYSTEM, "a"),
                    new ScopedName(MetadataScope.SYSTEM, "b"),
                    new ScopedName(MetadataScope.USER, "c")),
    ImmutableMap.of(new ScopedName(MetadataScope.SYSTEM, "x"), "1",
                    new ScopedName(MetadataScope.SYSTEM, "y"), "2",
                    new ScopedName(MetadataScope.USER, "z"), "4"));
  @Test
  public void testCreate() throws Exception {
    final MetadataEntity createEntity = MetadataEntity.builder().append("create", "test").build();
    createMetadataMutation(new MetadataMutation.Create(createEntity, testMetadata, CREATE_DIRECTIVES));

    Assert.assertEquals(testMetadata.getProperties(MetadataScope.SYSTEM),
                        getMetadataProperties(createEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.SYSTEM),
                        getMetadataTags(createEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.USER),
                        getMetadataProperties(createEntity, MetadataScope.USER));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.USER),
                        getMetadataTags(createEntity, MetadataScope.USER));

    // confirm that KEEP/PRESERVE work by trying to re-create the entity
    final Metadata recreate =
      new Metadata(Collections.EMPTY_SET,
                   ImmutableMap.of(new ScopedName(MetadataScope.SYSTEM, "x"), "10",
                                   new ScopedName(MetadataScope.SYSTEM, "y"), "20",
                                   new ScopedName(MetadataScope.USER, "z"), "40"));

    createMetadataMutation(new MetadataMutation.Create(createEntity, recreate, CREATE_DIRECTIVES));
    Assert.assertEquals(recreate.getProperties(MetadataScope.SYSTEM),
                        getMetadataProperties(createEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(recreate.getTags(MetadataScope.SYSTEM),
                        getMetadataTags(createEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.USER),
                        getMetadataProperties(createEntity, MetadataScope.USER));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.USER),
                        getMetadataTags(createEntity, MetadataScope.USER));
  }

  @Test
  public void testRemove() throws Exception {
    final MetadataEntity removeEntity = MetadataEntity.builder().append("remove", "test").build();
    createMetadataMutation(new MetadataMutation.Create(removeEntity, testMetadata, CREATE_DIRECTIVES));

    // confirm that the create was successful
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.SYSTEM),
                        getMetadataProperties(removeEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.SYSTEM),
                        getMetadataTags(removeEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.USER),
                        getMetadataProperties(removeEntity, MetadataScope.USER));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.USER),
                        getMetadataTags(removeEntity, MetadataScope.USER));

    // Remove only USER metadata
    removeMetadataMutation(new MetadataMutation.Remove(removeEntity, MetadataScope.USER));
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.SYSTEM),
                        getMetadataProperties(removeEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.SYSTEM),
                        getMetadataTags(removeEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(Collections.EMPTY_MAP,
                        getMetadataProperties(removeEntity, MetadataScope.USER));
    Assert.assertEquals(Collections.EMPTY_SET,
                        getMetadataTags(removeEntity, MetadataScope.USER));

    // Remove all metadata
    removeMetadataMutation(new MetadataMutation.Remove(removeEntity));
    Assert.assertEquals(Collections.EMPTY_MAP,
                        getMetadataProperties(removeEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(Collections.EMPTY_SET,
                        getMetadataTags(removeEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(Collections.EMPTY_MAP,
                        getMetadataProperties(removeEntity, MetadataScope.USER));
    Assert.assertEquals(Collections.EMPTY_SET,
                        getMetadataTags(removeEntity, MetadataScope.USER));
  }

  @Test
  public void testUpdate() throws Exception {
    final MetadataEntity updateEntity = MetadataEntity.builder().append("update", "test").build();
    createMetadataMutation(new MetadataMutation.Create(updateEntity, testMetadata, CREATE_DIRECTIVES));

    // confirm that the create was successful
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.SYSTEM),
                        getMetadataProperties(updateEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.SYSTEM),
                        getMetadataTags(updateEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.USER),
                        getMetadataProperties(updateEntity, MetadataScope.USER));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.USER),
                        getMetadataTags(updateEntity, MetadataScope.USER));

    Metadata update = new Metadata(Collections.EMPTY_SET,
                                   ImmutableMap.of(new ScopedName(MetadataScope.SYSTEM, "x"), "10",
                                                   new ScopedName(MetadataScope.SYSTEM, "y"), "11",
                                                   new ScopedName(MetadataScope.USER, "z"), "4"));

    updateMetadataMutation(new MetadataMutation.Update(updateEntity, update));

    Assert.assertEquals(update.getProperties(MetadataScope.SYSTEM),
                        getMetadataProperties(updateEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.SYSTEM),
                        getMetadataTags(updateEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(update.getProperties(MetadataScope.USER),
                        getMetadataProperties(updateEntity, MetadataScope.USER));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.USER),
                        getMetadataTags(updateEntity, MetadataScope.USER));
  }

  @Test
  public void testDrop() throws Exception {
    final MetadataEntity dropEntity = MetadataEntity.builder().append("drop", "test").build();
    createMetadataMutation(new MetadataMutation.Create(dropEntity, testMetadata, CREATE_DIRECTIVES));

    // confirm that the create was successful
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.SYSTEM),
                        getMetadataProperties(dropEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.SYSTEM),
                        getMetadataTags(dropEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(testMetadata.getProperties(MetadataScope.USER),
                        getMetadataProperties(dropEntity, MetadataScope.USER));
    Assert.assertEquals(testMetadata.getTags(MetadataScope.USER),
                        getMetadataTags(dropEntity, MetadataScope.USER));

    dropMetadataMutation(new MetadataMutation.Drop(dropEntity));

    Assert.assertEquals(Collections.EMPTY_MAP,
                        getMetadataProperties(dropEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(Collections.EMPTY_SET,
                        getMetadataTags(dropEntity, MetadataScope.SYSTEM));
    Assert.assertEquals(Collections.EMPTY_MAP,
                        getMetadataProperties(dropEntity, MetadataScope.USER));
    Assert.assertEquals(Collections.EMPTY_SET,
                        getMetadataTags(dropEntity, MetadataScope.USER));
  }
}
