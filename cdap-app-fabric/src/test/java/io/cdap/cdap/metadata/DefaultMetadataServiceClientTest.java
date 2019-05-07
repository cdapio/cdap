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
import io.cdap.cdap.spi.metadata.MetadataConstants;
import io.cdap.cdap.spi.metadata.MetadataDirective;
import io.cdap.cdap.spi.metadata.MetadataKind;
import io.cdap.cdap.spi.metadata.MetadataMutation;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.ScopedNameOfKind;
import org.junit.Test;

import java.util.Map;

public class DefaultMetadataServiceClientTest extends AppFabricTestBase {
  // - keep description if new metadata does not contain it
  // - preserve creation-time if it exists in current metadata
  private static final Map<ScopedNameOfKind, MetadataDirective> CREATE_DIRECTIVES = ImmutableMap.of(
    new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.SYSTEM, MetadataConstants.DESCRIPTION_KEY),
    MetadataDirective.KEEP,
    new ScopedNameOfKind(MetadataKind.PROPERTY, MetadataScope.SYSTEM, MetadataConstants.CREATION_TIME_KEY),
    MetadataDirective.PRESERVE);

  private final MetadataEntity testEntity = MetadataEntity.builder().append("test", "value").build();

  @Test
  public void testCreate() {
    Metadata toCreate = new Metadata(ImmutableSet.of(new ScopedName(MetadataScope.SYSTEM, "a"),
                                                     new ScopedName(MetadataScope.SYSTEM, "b"),
                                                     new ScopedName(MetadataScope.USER, "c")),
                                     ImmutableMap.of(new ScopedName(MetadataScope.SYSTEM, "x"), "1",
                                                     new ScopedName(MetadataScope.SYSTEM, "y"), "2",
                                                     new ScopedName(MetadataScope.USER, "x"), "3",
                                                     new ScopedName(MetadataScope.USER, "z"), "4"));

    createMetadataMutation(new MetadataMutation.Create(testEntity, toCreate, CREATE_DIRECTIVES));
  }

  @Test
  public void testRemove() {
    removeMetadataMutation(new MetadataMutation.Remove(testEntity, MetadataScope.USER));
  }

  @Test
  public void testUpdate() {
    Metadata toUpdate = new Metadata(ImmutableSet.of(new ScopedName(MetadataScope.USER, "d")),
                                     ImmutableMap.of(new ScopedName(MetadataScope.USER, "x"), "3",
                                                     new ScopedName(MetadataScope.USER, "z"), "4"));

    updateMetadataMutation(new MetadataMutation.Update(testEntity, toUpdate));
  }

  @Test
  public void testDrop() {
    dropMetadataMutation(new MetadataMutation.Drop(testEntity));
  }
}
