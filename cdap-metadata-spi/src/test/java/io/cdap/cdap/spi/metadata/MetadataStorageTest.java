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

package io.cdap.cdap.spi.metadata;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.cdap.cdap.api.annotation.Beta;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.spi.metadata.MetadataMutation.Create;
import io.cdap.cdap.spi.metadata.MetadataMutation.Drop;
import io.cdap.cdap.spi.metadata.MetadataMutation.Remove;
import io.cdap.cdap.spi.metadata.MetadataMutation.Update;
import io.cdap.cdap.test.SlowTests;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.Nullable;

import static io.cdap.cdap.api.metadata.MetadataScope.SYSTEM;
import static io.cdap.cdap.api.metadata.MetadataScope.USER;
import static io.cdap.cdap.spi.metadata.MetadataConstants.CREATION_TIME_KEY;
import static io.cdap.cdap.spi.metadata.MetadataConstants.DESCRIPTION_KEY;
import static io.cdap.cdap.spi.metadata.MetadataConstants.ENTITY_NAME_KEY;
import static io.cdap.cdap.spi.metadata.MetadataConstants.TTL_KEY;
import static io.cdap.cdap.spi.metadata.MetadataKind.PROPERTY;
import static io.cdap.cdap.spi.metadata.MetadataKind.TAG;

/**
 * Tests for Metadata SPI implementations.
 */
@Beta
public abstract class MetadataStorageTest {

  private static final String TYPE_ARTIFACT = MetadataEntity.ARTIFACT;
  private static final String TYPE_DATASET = MetadataEntity.DATASET;
  private static final String TYPE_PROGRAM = MetadataEntity.PROGRAM;
  private static final String DEFAULT_NAMESPACE = "default";
  private static final String SYSTEM_NAMESPACE = "system";

  private static final MutationOptions ASYNC = MutationOptions.builder().setAsynchronous(true).build();

  protected abstract MetadataStorage getMetadataStorage();

  @After
  public void ensureCleanUp() throws Exception {
    assertEmpty(getMetadataStorage(), SearchRequest.of("*")
      .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC)).setShowHidden(true).build());
  }

  @Test
  public void testMutations() throws IOException {
    MetadataStorage mds = getMetadataStorage();
    MetadataEntity entity = ofDataset(DEFAULT_NAMESPACE, "entity");

    // get metadata for non-existing entity
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // drop metadata for non-existing entity succeeds
    MetadataChange change = mds.apply(new Drop(entity), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // remove metadata for non-existing entity succeeds
    mds.apply(new Remove(entity, ImmutableSet.of(
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st1"),
      new ScopedNameOfKind(MetadataKind.TAG, USER,   "ut1"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"),
      new ScopedNameOfKind(PROPERTY, USER,   "up2"))), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // update metadata for non-existing entity creates it
    Metadata metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "a"),
                      new ScopedName(USER,   "b")),
      ImmutableMap.of(new ScopedName(SYSTEM, "p"), "v",
                      new ScopedName(USER,   "k"), "v1"));
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // test that update is idempotent
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, metadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // create metadata replaces existing metadata
    Metadata previousMetadata = metadata;
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(USER,   "ut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"), "sv1",
                      new ScopedName(SYSTEM, "sp2"), "sv2",
                      new ScopedName(USER,   "up1"), "uv1",
                      new ScopedName(USER,   "up2"), "uv2"));
    MetadataMutation create = new Create(entity, metadata, Collections.emptyMap());
    change = mds.apply(create, MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);

    // verify the metadata with variations of scope and kind
    verifyMetadata(mds, entity, metadata);
    // verify the metadata with a select subset of tags and properties
    verifyMetadataSelection(mds, entity, metadata, ImmutableSet.of(
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp1"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "nosuch"),
      new ScopedNameOfKind(PROPERTY, USER,   "up2"),
      new ScopedNameOfKind(PROPERTY, USER,   "nosuch"),
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "st1"),
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "nosuch"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "ut1"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nosuch")));
    // verify that a non-matching set tags and properties returns empty metadata
    verifyMetadataSelection(mds, entity, metadata, ImmutableSet.of(
      new ScopedNameOfKind(PROPERTY, SYSTEM, "nosuch"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nosuch")));

    // replace the system metadata with directives, user metadata should remain unchanged
    Metadata recreatedMetadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "nst0")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "nsp0"), "sv0"));
    MetadataMutation recreate = new Create(entity, recreatedMetadata, ImmutableMap.of(
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st1"), MetadataDirective.KEEP,
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st2"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp1"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"), MetadataDirective.KEEP));
    previousMetadata = metadata;
    // this is the new metadata according to directives
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(SYSTEM, "nst0"),
                      new ScopedName(USER,   "ut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "sv1",
                      new ScopedName(SYSTEM, "sp2"),  "sv2",
                      new ScopedName(SYSTEM, "nsp0"), "sv0",
                      new ScopedName(USER,   "up1"), "uv1",
                      new ScopedName(USER,   "up2"), "uv2"));
    change = mds.apply(recreate, MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // replace the metadata with directives
    recreatedMetadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "nut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "nsp2"), "sv2",
                      new ScopedName(USER,   "up3"),  "uv3"));
    recreate = new Create(entity, recreatedMetadata, ImmutableMap.of(
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st1"), MetadataDirective.KEEP,
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st2"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp1"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"), MetadataDirective.KEEP,
      new ScopedNameOfKind(PROPERTY, USER, "up2"), MetadataDirective.PRESERVE));
    previousMetadata = metadata;
    // this is the new metadata according to directives
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "nut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "sv1",
                      new ScopedName(SYSTEM, "sp2"),  "sv2",
                      new ScopedName(SYSTEM, "nsp2"), "sv2",
                      new ScopedName(USER,   "up2"),  "uv2",
                      new ScopedName(USER,   "up3"),  "uv3"));
    change = mds.apply(recreate, MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // update some tags and properties
    MetadataMutation update = new Update(entity, new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "ast1"),
                      new ScopedName(USER,   "aut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "nsp2"), "nsv2",
                      new ScopedName(USER,   "up2") , "nuv2",
                      new ScopedName(USER,   "up3"),  "uv3")));
    // verify new metadata after update
    previousMetadata = metadata;
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "ast1"),
                      new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "aut1"),
                      new ScopedName(USER,   "nut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "sp2"),  "sv2",
                      new ScopedName(SYSTEM, "nsp2"), "nsv2",
                      new ScopedName(USER,   "up2"),  "nuv2",
                      new ScopedName(USER,   "up3"),  "uv3"));
    change = mds.apply(update, MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // test that update is idempotent
    change = mds.apply(update, MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, metadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // remove some tags and properties
    MetadataMutation remove = new Remove(entity, ImmutableSet.of(
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "st1"),
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "st2"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nut1"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nosuch"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "nsp2"),
      new ScopedNameOfKind(PROPERTY, USER,   "up2")));
    previousMetadata = metadata;
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "ast1"),
                      new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "aut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(USER,   "up3"),  "uv3"));
    change = mds.apply(remove, MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // test that remove is idemtpotent
    change = mds.apply(remove, MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, metadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // drop all metadata for the entity
    change = mds.apply(new Drop(entity), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, metadata, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // drop is idempotent
    change = mds.apply(new Drop(entity), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);
  }

  @Test
  public void testAsyncMutations() throws Exception {
    MetadataStorage mds = getMetadataStorage();
    MetadataEntity entity = ofDataset(DEFAULT_NAMESPACE, "entity");

    // get metadata for non-existing entity
    verifyMetadataAsync(mds, entity, Metadata.EMPTY);

    // drop metadata for non-existing entity succeeds
    MetadataChange change = mds.apply(new Drop(entity), ASYNC);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadataAsync(mds, entity, Metadata.EMPTY);

    // remove metadata for non-existing entity succeeds
    mds.apply(new Remove(entity, ImmutableSet.of(
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st1"),
      new ScopedNameOfKind(MetadataKind.TAG, USER,   "ut1"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"),
      new ScopedNameOfKind(PROPERTY, USER,   "up2"))), ASYNC);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadataAsync(mds, entity, Metadata.EMPTY);

    // update metadata for non-existing entity creates it
    Metadata metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "a"),
                      new ScopedName(USER,   "b")),
      ImmutableMap.of(new ScopedName(SYSTEM, "p"), "v",
                      new ScopedName(USER,   "k"), "v1"));
    change = mds.apply(new Update(entity, metadata), ASYNC);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // test that update is idempotent
    change = mds.apply(new Update(entity, metadata), ASYNC);
    Assert.assertEquals(new MetadataChange(entity, metadata, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // create metadata replaces existing metadata
    Metadata previousMetadata = metadata;
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(USER,   "ut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"), "sv1",
                      new ScopedName(SYSTEM, "sp2"), "sv2",
                      new ScopedName(USER,   "up1"), "uv1",
                      new ScopedName(USER,   "up2"), "uv2"));
    MetadataMutation create = new Create(entity, metadata, Collections.emptyMap());
    change = mds.apply(create, ASYNC);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);

    // verify the metadata with variations of scope and kind
    verifyMetadataAsync(mds, entity, metadata);
    // verify the metadata with a select subset of tags and properties
    verifyMetadataSelectionAsync(mds, entity, metadata, ImmutableSet.of(
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp1"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "nosuch"),
      new ScopedNameOfKind(PROPERTY, USER,   "up2"),
      new ScopedNameOfKind(PROPERTY, USER,   "nosuch"),
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "st1"),
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "nosuch"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "ut1"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nosuch")));
    // verify that a non-matching set tags and properties returns empty metadata
    verifyMetadataSelectionAsync(mds, entity, metadata, ImmutableSet.of(
      new ScopedNameOfKind(PROPERTY, SYSTEM, "nosuch"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nosuch")));

    // replace the system metadata with directives, user metadata should remain unchanged
    Metadata recreatedMetadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "nst0")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "nsp0"), "sv0"));
    MetadataMutation recreate = new Create(entity, recreatedMetadata, ImmutableMap.of(
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st1"), MetadataDirective.KEEP,
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st2"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp1"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"), MetadataDirective.KEEP));
    previousMetadata = metadata;
    // this is the new metadata according to directives
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(SYSTEM, "nst0"),
                      new ScopedName(USER,   "ut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "sv1",
                      new ScopedName(SYSTEM, "sp2"),  "sv2",
                      new ScopedName(SYSTEM, "nsp0"), "sv0",
                      new ScopedName(USER,   "up1"), "uv1",
                      new ScopedName(USER,   "up2"), "uv2"));
    change = mds.apply(recreate, ASYNC);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // replace the metadata with directives
    recreatedMetadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "nut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "nsp2"), "sv2",
                      new ScopedName(USER,   "up3"),  "uv3"));
    recreate = new Create(entity, recreatedMetadata, ImmutableMap.of(
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st1"), MetadataDirective.KEEP,
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st2"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp1"), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"), MetadataDirective.KEEP,
      new ScopedNameOfKind(PROPERTY, USER, "up2"), MetadataDirective.PRESERVE));
    previousMetadata = metadata;
    // this is the new metadata according to directives
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "nut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "sv1",
                      new ScopedName(SYSTEM, "sp2"),  "sv2",
                      new ScopedName(SYSTEM, "nsp2"), "sv2",
                      new ScopedName(USER,   "up2"),  "uv2",
                      new ScopedName(USER,   "up3"),  "uv3"));
    change = mds.apply(recreate, ASYNC);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // update some tags and properties
    MetadataMutation update = new Update(entity, new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "ast1"),
                      new ScopedName(USER,   "aut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "nsp2"), "nsv2",
                      new ScopedName(USER,   "up2") , "nuv2",
                      new ScopedName(USER,   "up3"),  "uv3")));
    // verify new metadata after update
    previousMetadata = metadata;
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "ast1"),
                      new ScopedName(SYSTEM, "st1"),
                      new ScopedName(SYSTEM, "st2"),
                      new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "aut1"),
                      new ScopedName(USER,   "nut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "sp2"),  "sv2",
                      new ScopedName(SYSTEM, "nsp2"), "nsv2",
                      new ScopedName(USER,   "up2"),  "nuv2",
                      new ScopedName(USER,   "up3"),  "uv3"));
    change = mds.apply(update, ASYNC);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // test that update is idempotent
    change = mds.apply(update, ASYNC);
    Assert.assertEquals(new MetadataChange(entity, metadata, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // remove some tags and properties
    MetadataMutation remove = new Remove(entity, ImmutableSet.of(
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "st1"),
      new ScopedNameOfKind(MetadataKind.TAG,      SYSTEM, "st2"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nut1"),
      new ScopedNameOfKind(MetadataKind.TAG,      USER,   "nosuch"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "nsp2"),
      new ScopedNameOfKind(PROPERTY, USER,   "up2")));
    previousMetadata = metadata;
    metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "ast1"),
                      new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "aut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(USER,   "up3"),  "uv3"));
    change = mds.apply(remove, ASYNC);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // test that remove is idemtpotent
    change = mds.apply(remove, ASYNC);
    Assert.assertEquals(new MetadataChange(entity, metadata, metadata), change);
    verifyMetadataAsync(mds, entity, metadata);

    // drop all metadata for the entity
    change = mds.apply(new Drop(entity), ASYNC);
    Assert.assertEquals(new MetadataChange(entity, metadata, Metadata.EMPTY), change);
    verifyMetadataAsync(mds, entity, Metadata.EMPTY);

    // drop is idempotent
    change = mds.apply(new Drop(entity), ASYNC);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadataAsync(mds, entity, Metadata.EMPTY);
  }

  @Test
  public void testEmptyBatch() throws IOException {
    getMetadataStorage().batch(new ArrayList<>(), MutationOptions.DEFAULT);
  }

  @Test
  public void testBatch() throws IOException {
    MetadataEntity entity = MetadataEntity.ofDataset("a", "b");
    Map<ScopedNameOfKind, MetadataDirective> directives = ImmutableMap.of(
      new ScopedNameOfKind(PROPERTY, SYSTEM, CREATION_TIME_KEY), MetadataDirective.PRESERVE,
      new ScopedNameOfKind(PROPERTY, SYSTEM, DESCRIPTION_KEY), MetadataDirective.KEEP);
    MetadataStorage mds = getMetadataStorage();
    Create create = new Create(entity, new Metadata(SYSTEM, tags("batch"), props(
      CREATION_TIME_KEY, "12345678",
      DESCRIPTION_KEY, "hello",
      "other", "value")), directives);
    MetadataChange change = mds.apply(create, MutationOptions.DEFAULT);
    Assert.assertEquals(Metadata.EMPTY, change.getBefore());
    Assert.assertEquals(create.getMetadata(), change.getAfter());

    List<MetadataMutation> mutations = ImmutableList.of(
      new Update(entity, new Metadata(USER, tags("tag1", "tag2"))),
      new Drop(entity),
      new Create(entity, new Metadata(SYSTEM, tags("batch"), props(
        CREATION_TIME_KEY, "23456789",
        "other", "different")), directives),
      new Update(entity, new Metadata(USER, tags("tag3"), props("key", "value"))),
      new Remove(entity, ImmutableSet.of(
        new ScopedNameOfKind(PROPERTY, SYSTEM, "other"),
        new ScopedNameOfKind(TAG, USER, "tag2"))),
      new Create(entity, new Metadata(SYSTEM, tags("realtime"), props(
        CREATION_TIME_KEY, "33456789",
        DESCRIPTION_KEY, "new description",
        "other", "yet other")), directives)
    );

    // apply all mutations in sequence
    List<MetadataChange> changes = mutations.stream().map(mutation -> {
      try {
        return mds.apply(mutation, MutationOptions.DEFAULT);
      } catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }).collect(Collectors.toList());

    // drop and recreate the entity
    mds.apply(new Drop(entity), MutationOptions.DEFAULT);
    change = mds.apply(create, MutationOptions.DEFAULT);
    Assert.assertEquals(Metadata.EMPTY, change.getBefore());
    Assert.assertEquals(create.getMetadata(), change.getAfter());

    // apply all mutations in batch
    List<MetadataChange> batchChanges = mds.batch(mutations, MutationOptions.DEFAULT);

    // make sure the same mutations were applied
    Assert.assertEquals(changes, batchChanges);

    // clean up
    mds.apply(new Drop(entity), MutationOptions.DEFAULT);
  }

  @Test
  public void testUpdateRemove() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity entity = ofDataset(DEFAULT_NAMESPACE, "entity");

    // get metadata for non-existing entity
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // add some metadata for the entity
    Metadata metadata = new Metadata(ImmutableSet.of(new ScopedName(SYSTEM, "sysTag"),
                                                     new ScopedName(USER, "userTag")),
                                     ImmutableMap.of(new ScopedName(SYSTEM, "sysProp"), "sysVal",
                                                     new ScopedName(USER, "userProp"), "userVal"));
    MetadataChange change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // remove everything
    change = mds.apply(new Remove(entity), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, metadata, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // add back all metadata, then remove everything in user scope
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, USER), MutationOptions.DEFAULT);
    Metadata newMetadata = filterBy(metadata, SYSTEM, null);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove everything in system scope
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, SYSTEM), MutationOptions.DEFAULT);
    newMetadata = filterBy(metadata, USER, null);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all tags
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, TAG), MutationOptions.DEFAULT);
    newMetadata = filterBy(metadata, null, PROPERTY);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all properties
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, PROPERTY), MutationOptions.DEFAULT);
    newMetadata = filterBy(metadata, null, TAG);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all properties in system scope
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, SYSTEM, PROPERTY), MutationOptions.DEFAULT);
    newMetadata = union(filterBy(metadata, SYSTEM, TAG), filterBy(metadata, USER, null));
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all tags in user scope
    change = mds.apply(new Update(entity, metadata), MutationOptions.DEFAULT);
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, USER, TAG), MutationOptions.DEFAULT);
    newMetadata = union(filterBy(metadata, USER, PROPERTY), filterBy(metadata, SYSTEM, null));
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // clean up
    mds.apply(new Drop(entity), MutationOptions.DEFAULT);
  }

  @Test
  public void testVersionLessEntities() throws Exception {
    MetadataEntity appWithoutVersion = ofAppNoVersion("nn", "app");
    MetadataEntity appWithVersion =
      MetadataEntity.builder(appWithoutVersion).append(MetadataEntity.VERSION, "42").build();
    MetadataEntity appWithDefaultVersion =
      MetadataEntity.builder(appWithoutVersion).append(MetadataEntity.VERSION, "-SNAPSHOT").build();
    testVersionLessEntities(appWithoutVersion, appWithVersion, appWithDefaultVersion);

    MetadataEntity programWithoutVersion = MetadataEntity.builder(appWithoutVersion)
      .append(MetadataEntity.TYPE, "Service").appendAsType(MetadataEntity.PROGRAM, "pingService").build();
    MetadataEntity programWithVersion = MetadataEntity.builder(appWithVersion)
      .append(MetadataEntity.TYPE, "Service").appendAsType(MetadataEntity.PROGRAM, "pingService").build();
    MetadataEntity programWithDefaultVersion = MetadataEntity.builder(appWithDefaultVersion)
      .append(MetadataEntity.TYPE, "Service").appendAsType(MetadataEntity.PROGRAM, "pingService").build();
    testVersionLessEntities(programWithoutVersion, programWithVersion, programWithDefaultVersion);

    MetadataEntity scheduleWithoutVersion = MetadataEntity.builder(appWithoutVersion)
      .appendAsType(MetadataEntity.SCHEDULE, "pingSchedule").build();
    MetadataEntity scheduleWithVersion = MetadataEntity.builder(appWithVersion)
      .appendAsType(MetadataEntity.SCHEDULE, "pingSchedule").build();
    MetadataEntity scheduleWithDefaultVersion = MetadataEntity.builder(appWithDefaultVersion)
      .appendAsType(MetadataEntity.SCHEDULE, "pingSchedule").build();
    testVersionLessEntities(scheduleWithoutVersion, scheduleWithVersion, scheduleWithDefaultVersion);

    // artifacts have version but it is not ignored
    MetadataEntity artifactWithVersion = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "nn")
      .append(MetadataEntity.ARTIFACT, "artifact").append(MetadataEntity.VERSION, "42").build();
    MetadataEntity artifactWithDefaultVersion = MetadataEntity.builder().append(MetadataEntity.NAMESPACE, "nn")
      .append(MetadataEntity.ARTIFACT, "artifact").append(MetadataEntity.VERSION, "-SNAPSHOT").build();

    MetadataStorage mds = getMetadataStorage();
    Metadata meta = new Metadata(ImmutableSet.of(new ScopedName(SYSTEM, "sys"), new ScopedName(USER, "usr")),
                                 ImmutableMap.of(new ScopedName(SYSTEM, "sp"), "sv", new ScopedName(USER, "up"), "uv"));

    // metadata can only be retrieved with the matching version
    mds.apply(new Create(artifactWithVersion, meta, ImmutableMap.of()), MutationOptions.DEFAULT);
    Assert.assertEquals(meta, mds.read(new Read(artifactWithVersion)));
    Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(artifactWithDefaultVersion)));

    // metadata can only be dropped with the matching version
    mds.apply(new Drop(artifactWithDefaultVersion), MutationOptions.DEFAULT);
    Assert.assertEquals(meta, mds.read(new Read(artifactWithVersion)));
    Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(artifactWithDefaultVersion)));

    // search returns the exact version
    assertResults(mds, SearchRequest.of("sp:sv").build(), new MetadataRecord(artifactWithVersion, meta));

    // actually drop it
    mds.apply(new Drop(artifactWithVersion), MutationOptions.DEFAULT);
    Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(artifactWithVersion)));
    Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(artifactWithDefaultVersion)));
  }

  private void testVersionLessEntities(MetadataEntity withoutVersion, MetadataEntity withVersion,
                                       MetadataEntity withDefaultVersion) throws IOException {

    Metadata meta = new Metadata(ImmutableSet.of(new ScopedName(SYSTEM, "sys"), new ScopedName(USER, "usr")),
                                 ImmutableMap.of(new ScopedName(SYSTEM, "sp"), "sv", new ScopedName(USER, "up"), "uv"));

    MetadataStorage mds = getMetadataStorage();
    mds.apply(new Create(withVersion, meta, ImmutableMap.of()), MutationOptions.DEFAULT);
    Assert.assertEquals(meta, mds.read(new Read(withVersion)));
    Assert.assertEquals(meta, mds.read(new Read(withoutVersion)));
    Assert.assertEquals(meta, mds.read(new Read(withDefaultVersion)));

    // update with versioned entity
    Metadata meta2 = new Metadata(USER, ImmutableSet.of("plus"), ImmutableMap.of("plus", "minus"));
    mds.apply(new Update(withVersion, meta2), MutationOptions.DEFAULT);

    Metadata newMeta = union(meta, meta2);
    Assert.assertEquals(newMeta, mds.read(new Read(withVersion)));
    Assert.assertEquals(newMeta, mds.read(new Read(withoutVersion)));
    Assert.assertEquals(newMeta, mds.read(new Read(withDefaultVersion)));

    // remove a property with default version
    mds.apply(new Remove(withDefaultVersion,
                         ImmutableSet.of(new ScopedNameOfKind(TAG, USER, "usr"),
                                         new ScopedNameOfKind(PROPERTY, SYSTEM, "sp"))), MutationOptions.DEFAULT);
    newMeta = new Metadata(ImmutableSet.of(new ScopedName(SYSTEM, "sys"), new ScopedName(USER, "plus")),
                           ImmutableMap.of(new ScopedName(USER, "up"), "uv", new ScopedName(USER, "plus"), "minus"));
    Assert.assertEquals(newMeta, mds.read(new Read(withVersion)));
    Assert.assertEquals(newMeta, mds.read(new Read(withoutVersion)));
    Assert.assertEquals(newMeta, mds.read(new Read(withDefaultVersion)));

    // search should return default version
    assertResults(mds, SearchRequest.of("plus").build(), new MetadataRecord(withDefaultVersion, newMeta));

    // drop entity, verify it's gone for all version variations
    mds.batch(batch(new Drop(withoutVersion)), MutationOptions.DEFAULT);
    Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(withVersion)));
    Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(withoutVersion)));
    Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(withDefaultVersion)));
  }

  @Test
  public void testSearchDescription() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity app1 = ofApp("ns1", "app1");
    MetadataEntity app2 = ofApp("ns2", "app1");

    ScopedName descUser = new ScopedName(USER, "description");
    ScopedName descSystem = new ScopedName(SYSTEM, "description");
    MetadataRecord app1Record = new MetadataRecord(app1, new Metadata(tags(), props(descSystem,
                                                                                    "this is the first application",
                                                                                    descUser,
                                                                                    "business description of app1")));
    MetadataRecord app2Record = new MetadataRecord(app2, new Metadata(tags(), props(descSystem,
                                                                                    "this other app description")));
    // add some metadata with descriptions
    mds.batch(batch(new Update(app1, app1Record.getMetadata()),
                    new Update(app2, app2Record.getMetadata())), MutationOptions.DEFAULT);

    // search with field description:
    assertEmpty(mds, SearchRequest.of("description:occursnot").setShowHidden(true).build());
    assertResults(mds, SearchRequest.of("description:first").build(), app1Record);
    assertResults(mds, SearchRequest.of("description:this").build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("description:app*").build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("description:description").build(), app1Record, app2Record);

    // search in SCOPE SYSTEM
    assertResults(mds, SearchRequest.of("description:first").setScope(SYSTEM).build(), app1Record);
    assertResults(mds, SearchRequest.of("description:this").setScope(SYSTEM).build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("description:app*").setScope(SYSTEM).build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("description:description").setScope(SYSTEM).build(), app2Record);

    // search in scope USER
    assertEmpty(mds, SearchRequest.of("description:first").setScope(USER).build());
    assertEmpty(mds, SearchRequest.of("description:this").setScope(USER).build());
    assertResults(mds, SearchRequest.of("description:app*").setScope(USER).build(), app1Record);
    assertResults(mds, SearchRequest.of("description:description").setScope(USER).build(), app1Record);

    // search plain text should match description text 
    assertResults(mds, SearchRequest.of("first").build(), app1Record);
    assertResults(mds, SearchRequest.of("this").build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("app*").build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("description").build(), app1Record, app2Record);

    // search in SCOPE SYSTEM
    assertResults(mds, SearchRequest.of("first").setScope(SYSTEM).build(), app1Record);
    assertResults(mds, SearchRequest.of("this").setScope(SYSTEM).build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("app*").setScope(SYSTEM).build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("description").setScope(SYSTEM).build(), app2Record);

    // search in scope USER
    assertEmpty(mds, SearchRequest.of("first").setScope(USER).build());
    assertEmpty(mds, SearchRequest.of("this").setScope(USER).build());
    assertResults(mds, SearchRequest.of("app*").setScope(USER).build(), app1Record);
    assertResults(mds, SearchRequest.of("description").setScope(USER).build(), app1Record);

    mds.batch(batch(new Drop(app1), new Drop(app2)), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchOnTTL() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity ds = ofDataset("ns1", "ds");
    Metadata metaWithTTL = new Metadata(SYSTEM, props(TTL_KEY, "3600"));
    MetadataRecord dsRecord = new MetadataRecord(ds, metaWithTTL);

    mds.apply(new Update(ds, metaWithTTL), MutationOptions.DEFAULT);
    assertEmpty(mds, SearchRequest.of("ttl:3600").setScope(USER).build());
    assertResults(mds, SearchRequest.of("ttl:3600").build(), dsRecord);
    assertResults(mds, SearchRequest.of("ttl:3600").setScope(SYSTEM).build(), dsRecord);

    List<String> moreQueries = new ArrayList<>(ImmutableList.of(
      "3600", "properties:ttl", "ttl:*", "TTL:3600"));
    moreQueries.addAll(getAdditionalTTLQueries());
    for (String query : moreQueries) {
      try {
        assertResults(mds, SearchRequest.of(query).build(), dsRecord);
      } catch (Throwable e) {
        throw new Exception("Search failed for query: " + query);
      }
    }

    mds.apply(new Drop(ds), MutationOptions.DEFAULT);
  }

  /**
   * Subclasses can override this to add more query strings to test with TTL.
   */
  protected List<String> getAdditionalTTLQueries() {
    return Collections.emptyList();
  }

  @Test
  public void testSearchOnSchema() throws IOException {
    Schema bytesArraySchema = Schema.arrayOf(Schema.of(Schema.Type.BYTES));
    Schema stringArraySchema = Schema.arrayOf(Schema.of(Schema.Type.STRING));
    Schema booleanBytesMapSchema = Schema.mapOf(Schema.of(Schema.Type.BOOLEAN), Schema.of(Schema.Type.BYTES));
    Schema nestedMapSchema = Schema.mapOf(bytesArraySchema, booleanBytesMapSchema);
    Schema record22Schema = Schema.recordOf("record22", Schema.Field.of("a", nestedMapSchema));
    Schema record22ArraySchema = Schema.arrayOf(record22Schema);
    Schema bytesDoubleMapSchema = Schema.mapOf(Schema.of(Schema.Type.BYTES), Schema.of(Schema.Type.DOUBLE));
    Schema record21Schema = Schema.recordOf("record21",
                                            Schema.Field.of("x", Schema.of(Schema.Type.STRING)),
                                            Schema.Field.of("y", stringArraySchema),
                                            Schema.Field.of("z", bytesDoubleMapSchema));
    Schema record21to22MapSchema = Schema.mapOf(record21Schema, record22ArraySchema);
    Schema nullableIntSchema = Schema.nullableOf(Schema.of(Schema.Type.INT));
    Schema tripeUnionSchema = Schema.unionOf(Schema.of(Schema.Type.INT), Schema.of(Schema.Type.LONG),
                                             Schema.of(Schema.Type.NULL));
    Schema complexSchema = Schema.recordOf("record1",
                                           Schema.Field.of("map1", record21to22MapSchema),
                                           Schema.Field.of("i", nullableIntSchema),
                                           Schema.Field.of("j", tripeUnionSchema));
    Schema anotherComplexSchema = Schema.arrayOf(Schema.of(Schema.Type.STRING));
    Schema superComplexSchema = Schema.unionOf(complexSchema, anotherComplexSchema, Schema.of(Schema.Type.NULL));

    String[] expectedTermsInIndex = {
      "record1", "record1:RECORD",
      "map1", "map1:MAP",
      "record21", "record21:RECORD",
      "x", "x:STRING",
      "y", "y:ARRAY",
      "z", "z:MAP",
      "record22", "record22:RECORD",
      "a", "a:MAP",
      "i", "i:INT",
      "j", "j:UNION",
    };

    MetadataEntity entity = MetadataEntity.ofDataset("myDs");
    Metadata meta = new Metadata(SYSTEM, props(MetadataConstants.ENTITY_NAME_KEY, "myDs",
                                               MetadataConstants.SCHEMA_KEY, superComplexSchema.toString()));
    MetadataRecord record = new MetadataRecord(entity, meta);
    MetadataStorage mds = getMetadataStorage();

    mds.apply(new Update(entity, meta), MutationOptions.DEFAULT);
    assertResults(mds, SearchRequest.of("myds").build(), record);
    assertResults(mds, SearchRequest.of("schema:*").build(), record);
    assertResults(mds, SearchRequest.of("properties:schema").build(), record);

    for (String expectedTerm : expectedTermsInIndex) {
      assertResults(mds, SearchRequest.of(expectedTerm).build(), record);
      assertResults(mds, SearchRequest.of("schema:" + expectedTerm).build(), record);
    }

    // clean up
    mds.apply(new Drop(entity), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchWithInvalidSchema() throws IOException {
    String invalidSchema = "an invalid schema";
    MetadataEntity entity = MetadataEntity.ofDataset("myDs");
    Metadata meta = new Metadata(SYSTEM, props(MetadataConstants.ENTITY_NAME_KEY, "myDs",
                                               MetadataConstants.SCHEMA_KEY, invalidSchema));
    MetadataRecord record = new MetadataRecord(entity, meta);
    MetadataStorage mds = getMetadataStorage();

    mds.apply(new Update(entity, meta), MutationOptions.DEFAULT);
    assertResults(mds, SearchRequest.of("myds").build(), record);
    assertResults(mds, SearchRequest.of("schema:*").build(), record);
    assertResults(mds, SearchRequest.of("properties:schema").build(), record);
    assertResults(mds, SearchRequest.of("schema:inval*").build(), record);

    // clean up
    mds.apply(new Drop(entity), MutationOptions.DEFAULT);
  }

  @Test
  public void testCustomEntities() throws IOException {
    MetadataEntity dataset = MetadataEntity.ofDataset("different", "stuff");
    MetadataEntity hype = MetadataEntity.builder().append("field", "value").append("hype", "custom").build();
    MetadataEntity field = MetadataEntity.builder().appendAsType("field", "val").append("hype", "irrel").build();

    Set<String> tags = tags("foo", "bar");
    MetadataRecord datasetRecord =
      new MetadataRecord(dataset, new Metadata(SYSTEM, tags, props(ENTITY_NAME_KEY, "stuff")));
    MetadataRecord hypeRecord =
      new MetadataRecord(hype, new Metadata(SYSTEM, tags, props(ENTITY_NAME_KEY, "custom")));
    MetadataRecord fieldRecord =
      new MetadataRecord(field, new Metadata(SYSTEM, tags, props(ENTITY_NAME_KEY, "irrel")));

    // validate update and read
    MetadataStorage mds = getMetadataStorage();
    mds.batch(ImmutableList.of(new Update(dataset, datasetRecord.getMetadata()),
                               new Update(hype, hypeRecord.getMetadata()),
                               new Update(field, fieldRecord.getMetadata())), MutationOptions.DEFAULT);
    Assert.assertEquals(datasetRecord.getMetadata(), mds.read(new Read(dataset)));
    Assert.assertEquals(hypeRecord.getMetadata(), mds.read(new Read(hype)));
    Assert.assertEquals(fieldRecord.getMetadata(), mds.read(new Read(field)));

    // search with type filters
    assertResults(mds, SearchRequest.of("*").build(), datasetRecord, hypeRecord, fieldRecord);
    assertResults(mds, SearchRequest.of("*").addType("dataset").build(), datasetRecord);
    assertResults(mds, SearchRequest.of("*").addType("hype").build(), hypeRecord);
    assertResults(mds, SearchRequest.of("*").addType("field").build(), fieldRecord);
    assertResults(mds, SearchRequest.of("*").addType("field").addType("nosuch").build(), fieldRecord);
    assertResults(mds, SearchRequest.of("*").addType("field").addType("hype").build(), fieldRecord, hypeRecord);

    // search on the type field
    assertResults(mds, SearchRequest.of("hype:custom").build(), hypeRecord);
    assertResults(mds, SearchRequest.of("hype:cust*").build(), hypeRecord);
    assertResults(mds, SearchRequest.of("hype:*").build(), hypeRecord);
    assertEmpty(mds, SearchRequest.of("field:value").build());
    assertResults(mds, SearchRequest.of("field:val*").build(), fieldRecord);
    assertResults(mds, SearchRequest.of("field:*").build(), fieldRecord);

    // clean up
    mds.batch(batch(new Drop(dataset), new Drop(hype), new Drop(field)), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchOnTags() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    String ns1 = "ns1";
    String ns2 = "ns2";
    MetadataEntity app1 = ofApp(ns1, "app1");
    MetadataEntity app2 = ofApp(ns2, "app1");
    MetadataEntity program1 = ofWorker(app1, "wk1");
    MetadataEntity dataset1 = ofDataset(ns1, "ds1");
    MetadataEntity dataset2 = ofDataset(ns1, "ds2");
    MetadataEntity file1 = MetadataEntity.builder(dataset1).appendAsType("file", "f1").build();

    List<MetadataEntity> entities = ImmutableList.of(app1, app2, program1, dataset1, dataset2, file1);
    for (MetadataEntity entity : entities) {
      Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(entity)));
    }

    // add tags for these entities
    MetadataRecord app1Record = new MetadataRecord(app1, new Metadata(USER, tags("tag1", "tag2", "tag3")));
    MetadataRecord app2Record = new MetadataRecord(app2, new Metadata(USER, tags("tag1", "tag2", "tag3_more")));
    MetadataRecord program1Record = new MetadataRecord(program1, new Metadata(USER, tags("tag1")));
    MetadataRecord dataset1Record = new MetadataRecord(dataset1,
                                                       new Metadata(USER, tags("tag3", "tag2", "tag12-tag33")));
    MetadataRecord dataset2Record = new MetadataRecord(dataset2, new Metadata(USER, tags("tag2", "tag4")));
    MetadataRecord file1Record = new MetadataRecord(file1, new Metadata(USER, tags("tag2", "tag5")));

    mds.batch(ImmutableList.of(app1Record, app2Record, program1Record, dataset1Record, dataset2Record, file1Record)
                .stream().map(record -> new Update(record.getEntity(), record.getMetadata()))
                .collect(Collectors.toList()), MutationOptions.DEFAULT);

    // Try to search on all tags
    assertResults(mds, SearchRequest.of("tags:*").addNamespace(ns1).build(),
                  app1Record, program1Record, dataset1Record, dataset2Record, file1Record);

    // Try to search for tag1*
    assertResults(mds, SearchRequest.of("tags:tag1*").addNamespace(ns1).build(),
                  app1Record, program1Record, dataset1Record);

    // Try to search for tag1 with spaces in search query and mixed case of tags keyword
    assertResults(mds, SearchRequest.of("  tAGS  :  tag1  ").addNamespace(ns1).build(),
                  app1Record, program1Record);

    // Try to search for tag5
    assertResults(mds, SearchRequest.of("tags:tag5").addNamespace(ns1).build(),
                  file1Record);

    // Try to search for tag2
    assertResults(mds, SearchRequest.of("tags:tag2").addNamespace(ns1).build(),
                  app1Record, dataset1Record, dataset2Record, file1Record);

    // Try to search for tag4
    assertResults(mds, SearchRequest.of("tags:tag4").addNamespace(ns1).build(),
                  dataset2Record);

    // Try to search for tag33
    assertResults(mds, SearchRequest.of("tags:tag33").addNamespace(ns1).build(),
                  dataset1Record);

    // Try to search for a tag which has - in it
    assertResults(mds, SearchRequest.of("tag12-tag33").addNamespace(ns1).build(),
                  dataset1Record);

    // Try to search for tag33 with spaces in query
    assertResults(mds, SearchRequest.of("  tag33  ").addNamespace(ns1).build(),
                  dataset1Record);

    // Try wildcard search for tag3*
    assertResults(mds, SearchRequest.of("tags:tag3*").addNamespace(ns1).build(),
                  app1Record, dataset1Record);

    // try search in another namespace
    assertResults(mds, SearchRequest.of("tags:tag1").addNamespace(ns2).build(),
                  app2Record);

    assertResults(mds, SearchRequest.of("tag3").addNamespace(ns2).build(),
                  app2Record);

    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns2).build(),
                  app2Record);

    // try to search across namespaces
    assertResults(mds, SearchRequest.of("tags:tag1").build(),
                  app1Record, app2Record, program1Record);

    // cleanup
    mds.batch(entities.stream().map(Drop::new).collect(Collectors.toList()), MutationOptions.DEFAULT);

    // Search should be empty after deleting tags
    assertEmpty(mds, SearchRequest.of("*").setLimit(10).build());
  }

  @Test
  public void testSearchOnTagsUpdate() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity entity = ofWorkflow(ofApp(DEFAULT_NAMESPACE, "appX"), "wtf");
    Metadata meta = new Metadata(SYSTEM, tags("tag1", "tag2"));
    mds.apply(new Update(entity, meta), MutationOptions.DEFAULT);
    Assert.assertEquals(meta.getTags(SYSTEM), mds.read(new Read(entity, SYSTEM)).getTags(SYSTEM));
    assertResults(mds, SearchRequest.of("tag1").build(), new MetadataRecord(entity, meta));

    // add an more tags
    mds.apply(new Update(entity, new Metadata(SYSTEM, tags("tag3", "tag4"))), MutationOptions.DEFAULT);
    Set<String> newTags = tags("tag1", "tag2", "tag3", "tag4");
    Metadata newMeta = new Metadata(SYSTEM, newTags);
    Assert.assertEquals(newTags, mds.read(new Read(entity, SYSTEM)).getTags(SYSTEM));
    for (String expectedTag : newTags) {
      assertResults(mds, SearchRequest.of(expectedTag).build(), new MetadataRecord(entity, newMeta));
    }

    // add an empty set of tags. This should have no effect on retrieval or search of tags
    mds.apply(new Update(entity, new Metadata(SYSTEM, tags())), MutationOptions.DEFAULT);
    Assert.assertEquals(newTags, mds.read(new Read(entity, SYSTEM)).getTags(SYSTEM));
    for (String expectedTag : newTags) {
      assertResults(mds, SearchRequest.of(expectedTag).build(), new MetadataRecord(entity, newMeta));
    }

    // clean up
    mds.apply(new Drop(entity), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchOnTypes() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity myDs = ofDataset(DEFAULT_NAMESPACE, "myDs");
    MetadataEntity myField1 = MetadataEntity.builder(myDs).appendAsType("field", "myField1").build();
    MetadataEntity myField2 = MetadataEntity.builder(myDs).appendAsType("field", "myField2").build();
    MetadataRecord record1 = new MetadataRecord(myField1, new Metadata(USER, props("testKey1", "testValue1")));
    MetadataRecord record2 = new MetadataRecord(myField2, new Metadata(USER, props("testKey2", "testValue2")));

    mds.batch(batch(new Update(myField1, record1.getMetadata()), new Update(myField2, record2.getMetadata())),
              MutationOptions.DEFAULT);

    // Search for it based on value
    assertResults(mds, SearchRequest.of("field:myField1").build(), record1);

    // should return both fields
    assertResults(mds, SearchRequest.of("field:myFie*").build(), record1, record2);
    assertResults(mds, SearchRequest.of("field*").build(), record1, record2);

    // searching an invalid type should return nothing
    assertEmpty(mds, SearchRequest.of("x*").addType("invalid").build());

    // clean up
    mds.batch(batch(new Drop(myField1), new Drop(myField2)), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchOnValue() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity program = ofWorker(ofApp("ns1", "app1"), "wk1");
    MetadataEntity dataset = ofDataset("ns1", "ds2");

    // Add some metadata
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    MetadataRecord programRecord = new MetadataRecord(
      program, new Metadata(USER, props("key1", "value1", "key2", "value2", "multiword", multiWordValue)));
    mds.apply(new Update(program, programRecord.getMetadata()), MutationOptions.DEFAULT);

    // Search for it based on value
    assertResults(mds, SearchRequest.of("value1").build(), programRecord);
    assertResults(mds, SearchRequest.of("  aV1   ").addType(TYPE_PROGRAM).build(), programRecord);
    assertEmpty(mds, SearchRequest.of("  aV1   ").addType(TYPE_ARTIFACT).build());

    // Search for it based split patterns to make sure nothing is matched
    assertEmpty(mds, SearchRequest.of("-").build());
    assertEmpty(mds, SearchRequest.of(",").build());
    assertEmpty(mds, SearchRequest.of("_").build());
    assertEmpty(mds, SearchRequest.of(", ,").build());
    assertEmpty(mds, SearchRequest.of(", - ,").build());

    // Search for it based on a word in value
    assertResults(mds, SearchRequest.of("av5").addType(TYPE_PROGRAM).build(), programRecord);

    // Case insensitive
    assertResults(mds, SearchRequest.of("ValUe1").addType(TYPE_PROGRAM).build(), programRecord);

    // add a property for the program
    mds.apply(new Update(program, new Metadata(SYSTEM, props("key3", "value1"))), MutationOptions.DEFAULT);
    programRecord = new MetadataRecord(
      program, new Metadata(ImmutableSet.of(), ImmutableMap.of(new ScopedName(USER, "key1"), "value1",
                                                               new ScopedName(USER, "key2"), "value2",
                                                               new ScopedName(USER, "multiword"), multiWordValue,
                                                               new ScopedName(SYSTEM, "key3"), "value1")));
    // search by value
    assertResults(mds, SearchRequest.of("value1").addType(TYPE_PROGRAM).build(), programRecord);

    // add a property for the dataset
    MetadataRecord datasetRecord = new MetadataRecord(dataset, new Metadata(USER, props("key21", "value21")));
    mds.apply(new Update(dataset, datasetRecord.getMetadata()), MutationOptions.DEFAULT);

    // Search based on value prefix
    assertResults(mds, SearchRequest.of("value2*").build(), programRecord, datasetRecord);

    // Search based on value prefix in the wrong namespace
    assertEmpty(mds, SearchRequest.of("value2*").addNamespace("ns12").build());

    // clean up
    mds.batch(batch(new Drop(program), new Drop(dataset)), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchOnKeyValue() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity program = ofWorker(ofApp("ns1", "app1"), "wk1");
    MetadataEntity dataset = ofDataset("ns1", "ds2");

    // add properties for program and dataset
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    MetadataRecord programRecord = new MetadataRecord(program, new Metadata(USER, props("key1", "value1",
                                                                                        "key2", "value2",
                                                                                        "multiword", multiWordValue)));
    MetadataRecord datasetRecord = new MetadataRecord(dataset,
      new Metadata(ImmutableSet.of(), ImmutableMap.of(new ScopedName(SYSTEM, "sKey1"), "sValue1",
                                                      new ScopedName(USER, "Key1"), "Value1")));

    mds.batch(batch(new Update(program, programRecord.getMetadata()),
                    new Update(dataset, datasetRecord.getMetadata())), MutationOptions.DEFAULT);

    // search based on the names of properties
    assertResults(mds, SearchRequest.of("properties:key1").build(), programRecord, datasetRecord);
    assertResults(mds, SearchRequest.of("properties:skey1").setScope(SYSTEM).build(), datasetRecord);
    assertResults(mds, SearchRequest.of("properties:multi*").setScope(USER).build(), programRecord);

    // Search for it based on key and value
    assertResults(mds, SearchRequest.of("key1:value1").addType(TYPE_PROGRAM).build(), programRecord);
    assertResults(mds, SearchRequest.of("key1:value1").build(), programRecord, datasetRecord);

    // Search for it based on a word in value
    assertResults(mds, SearchRequest.of("multiword:aV5").build(), programRecord);

    // Search for it based on a word in value with spaces in search query
    assertResults(mds, SearchRequest.of("  multiword:aV1   ").build(), programRecord);

    // remove the multiword property
    mds.apply(new Remove(program, ImmutableSet.of(new ScopedNameOfKind(PROPERTY, USER, "multiword"))),
              MutationOptions.DEFAULT);
    programRecord = new MetadataRecord(program, new Metadata(USER, props("key1", "value1", "key2", "value2")));

    // search results should be empty after removing this key as the indexes are deleted
    assertEmpty(mds, SearchRequest.of("multiword:aV5").build());

    // Test wrong ns
    assertEmpty(mds, SearchRequest.of("key1:value1").addNamespace("ns12").build());

    // Test multi word query
    assertResults(mds, SearchRequest.of("  value1  av2  ").build(), programRecord, datasetRecord);
    assertResults(mds, SearchRequest.of("  value1  sValue1  ").build(), programRecord, datasetRecord);
    assertResults(mds, SearchRequest.of("  valu*  sVal*  ").build(), programRecord, datasetRecord);

    // search with explicit set of target types
    assertResults(mds, SearchRequest.of("  valu*  sVal*  ").addType(TYPE_PROGRAM).addType(TYPE_DATASET).build(),
                  programRecord, datasetRecord);
    // search all target types
    assertResults(mds, SearchRequest.of("  valu*  sVal*  ").build(), programRecord, datasetRecord);

    // clean up
    mds.batch(batch(new Drop(program), new Drop(dataset)), MutationOptions.DEFAULT);
  }

  @Test
  public void testUpdateSearch() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns = "ns";
    MetadataEntity program = ofWorker(ofApp(ns, "app1"), "wk1");

    Metadata meta = new Metadata(USER, tags("tag1", "tag2"), props("key1", "value1", "key2", "value2"));
    MetadataRecord programRecord = new MetadataRecord(program, meta);
    mds.apply(new Update(program, meta), MutationOptions.DEFAULT);

    assertResults(mds, SearchRequest.of("value1").addNamespace(ns).build(), programRecord);
    assertResults(mds, SearchRequest.of("value2").addNamespace(ns).build(), programRecord);
    assertResults(mds, SearchRequest.of("tag2").addNamespace(ns).build(), programRecord);

    mds.apply(new Update(program, new Metadata(USER, props("key1", "value3"))), MutationOptions.DEFAULT);
    mds.apply(new Remove(program, ImmutableSet.of(new ScopedNameOfKind(PROPERTY, USER, "key2"),
                                                  new ScopedNameOfKind(TAG, USER, "tag2"))),
              MutationOptions.DEFAULT);
    programRecord = new MetadataRecord(program, new Metadata(USER, tags("tag1"), props("key1", "value3")));

    // Searching for value1 should be empty
    assertEmpty(mds, SearchRequest.of("value1").addNamespace(ns).build());

    // Instead key1 has value value3 now
    assertResults(mds, SearchRequest.of("value3").addNamespace(ns).build(), programRecord);

    // key2 and tag2 were deleted
    assertEmpty(mds, SearchRequest.of("value2").addNamespace(ns).build());
    assertEmpty(mds, SearchRequest.of("tag2").addNamespace(ns).build());

    // tag1 is still here
    assertResults(mds, SearchRequest.of("tag1").addNamespace(ns).build(), programRecord);

    // clean up
    mds.apply(new Drop(program), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchIncludesSystemEntities() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns1 = "ns1";
    String ns2 = "ns2";
    MetadataEntity program = ofWorker(ofApp(ns1, "app1"), "wk1");
    // Use the same artifact in two different namespaces - system and ns2
    MetadataEntity artifact = ofArtifact(ns2, "artifact", "1.0");
    MetadataEntity sysArtifact = ofArtifact(SYSTEM_NAMESPACE, "artifact", "1.0");

    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Metadata meta = new Metadata(SYSTEM, props(multiWordKey, multiWordValue));

    MetadataRecord programRecord = new MetadataRecord(program, meta);
    MetadataRecord artifactRecord = new MetadataRecord(artifact, meta);
    MetadataRecord sysArtifactRecord = new MetadataRecord(sysArtifact, meta);

    mds.batch(batch(new Update(program, meta), new Update(artifact, meta), new Update(sysArtifact, meta)),
              MutationOptions.DEFAULT);

    // perform the exact same multiword search in the 'ns1' namespace. It should return the system artifact along with
    // matched entities in the 'ns1' namespace
    assertResults(mds, SearchRequest.of("aV5").addNamespace(ns1).addSystemNamespace().build(),
                  programRecord, sysArtifactRecord);

    // search only programs - should only return flow
    assertResults(mds, SearchRequest.of("aV5").addNamespace(ns1).addType(TYPE_PROGRAM).build(),
                  programRecord);

    assertResults(mds, SearchRequest.of("multiword:aV5").addNamespace(ns1).addType(TYPE_PROGRAM).build(),
                  programRecord);

    // search only artifacts - should only return system artifact
    assertResults(mds, SearchRequest.of("multiword:" + multiWordValue)
                    .addNamespace(ns1).addSystemNamespace().addType(TYPE_ARTIFACT).build(),
                  sysArtifactRecord);

    // search all entities in namespace 'ns2' - should return the system artifact and the same artifact in ns2
    assertResults(mds, SearchRequest.of("multiword:aV4").addNamespace(ns2).addSystemNamespace().build(),
                  artifactRecord, sysArtifactRecord);

    // search only programs in a namespace 'ns2'. Should return empty
    assertEmpty(mds, SearchRequest.of("aV*").addNamespace(ns2).addSystemNamespace().addType(TYPE_PROGRAM).build());

    // search all entities in non-existent namespace 'ns3'. Should return only the system artifact
    assertResults(mds, SearchRequest.of("av*").addNamespace("ns3").addSystemNamespace().build(),
                  sysArtifactRecord);

    // search the system namespace for all entities. Should return only the system artifact
    assertResults(mds, SearchRequest.of("av*").addSystemNamespace().build(),
                  sysArtifactRecord);

    // clean up
    mds.batch(batch(new Drop(program), new Drop(artifact), new Drop(sysArtifact)), MutationOptions.DEFAULT);
  }

  private List<? extends MetadataMutation> batch(MetadataMutation ... mutations) {
    return ImmutableList.copyOf(mutations);
  }

  @Test
  public void testSearchDifferentNamespaces() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns1 = "ns1";
    MetadataEntity artifact = ofArtifact(ns1, "artifact", "1.0");
    MetadataEntity sysArtifact = ofArtifact(SYSTEM_NAMESPACE, "artifact", "1.0");

    String multiWordKey = "multiword";
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Metadata meta = new Metadata(SYSTEM, props(multiWordKey, multiWordValue));

    MetadataRecord artifactRecord = new MetadataRecord(artifact, meta);
    MetadataRecord sysArtifactRecord = new MetadataRecord(sysArtifact, meta);

    mds.batch(batch(new Update(artifact, meta), new Update(sysArtifact, meta)), MutationOptions.DEFAULT);

    // searching only user namespace should not return system entity
    assertResults(mds, SearchRequest.of("aV5").addNamespace(ns1).build(), artifactRecord);

    // searching only user namespace and system should return only the system entity
    assertResults(mds, SearchRequest.of("aV5").addSystemNamespace().build(), sysArtifactRecord);

    // searching only user namespace and system should return both entities
    assertResults(mds, SearchRequest.of("aV5").addNamespace(ns1).addSystemNamespace().build(),
                  artifactRecord, sysArtifactRecord);

    // clean up
    mds.batch(batch(new Drop(artifact), new Drop(sysArtifact)), MutationOptions.DEFAULT);
  }

  @Test
  public void testCrossNamespaceSearch() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns1 = "ns1";
    String ns2 = "ns2";
    MetadataEntity ns1app1 = ofApp(ns1, "a1");
    MetadataEntity ns1app2 = ofApp(ns1, "a2");
    MetadataEntity ns1app3 = ofApp(ns1, "a3");
    MetadataEntity ns2app1 = ofApp(ns2, "a1");
    MetadataEntity ns2app2 = ofApp(ns2, "a2");

    MetadataRecord
      record11 = new MetadataRecord(ns1app1, new Metadata(USER, tags("v1"), props("k1", "v1"))),
      record12 = new MetadataRecord(ns1app2, new Metadata(USER, props("k1", "v1", "k2", "v2"))),
      record13 = new MetadataRecord(ns1app3, new Metadata(USER, props("k1", "v1", "k3", "v3"))),
      record21 = new MetadataRecord(ns2app1, new Metadata(USER, props("k1", "v1", "k2", "v2"))),
      record22 = new MetadataRecord(ns2app2, new Metadata(USER, tags("v2", "v3"), props("k1", "v1")));
    MetadataRecord[] records = { record11, record12, record13, record21, record22 };
    // apply all metadata in batch
    mds.batch(Arrays.stream(records)
                .map(record -> new Update(record.getEntity(), record.getMetadata())).collect(Collectors.toList()),
              MutationOptions.DEFAULT);

    // everything should match 'v1'
    assertResults(mds, SearchRequest.of("v1").setLimit(10).build(),
                  record11, record12, record13, record21, record22);

    // ns1app2, ns2app1, and ns2app2 should match 'v2'
    assertResults(mds, SearchRequest.of("v2").setLimit(10).build(),
                  record12, record21, record22);

    // ns1app3 and ns2app2 should match 'v3'
    assertResults(mds, SearchRequest.of("v3").setLimit(10).build(),
                  record13, record22);

    // clean up
    mds.batch(batch(
      new Drop(ns1app1), new Drop(ns1app2), new Drop(ns1app3), new Drop(ns2app1), new Drop(ns2app2)),
              MutationOptions.DEFAULT);
  }

  @Test
  public void testCrossNamespaceDefaultSearch() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity ns1app = ofApp("ns1", "a");
    MetadataEntity ns2app = ofApp("ns2", "a");

    MetadataRecord app1Record = new MetadataRecord(ns1app, new Metadata(USER, props("k1", "v1", "k2", "v2")));
    MetadataRecord app2Record = new MetadataRecord(ns2app, new Metadata(USER, props("k1", "v1")));

    mds.batch(batch(new Update(ns1app, app1Record.getMetadata()),
                    new Update(ns2app, app2Record.getMetadata())), MutationOptions.DEFAULT);

    assertResults(mds, SearchRequest.of("v1").build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("v2").build(), app1Record);
    assertResults(mds, SearchRequest.of("*").build(), app1Record, app2Record);

    // clean up
    mds.batch(batch(new Drop(ns1app), new Drop(ns2app)), MutationOptions.DEFAULT);
  }

  @Test
  public void testCrossNamespaceCustomSearch() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    String appName = "app";
    MetadataEntity ns1App = ofApp("ns1", appName);
    MetadataEntity ns2App = ofApp("ns2", appName);

    Metadata meta = new Metadata(SYSTEM, props(ENTITY_NAME_KEY, appName));
    MetadataRecord app1Record = new MetadataRecord(ns1App, meta);
    MetadataRecord app2Record = new MetadataRecord(ns2App, meta);

    mds.batch(batch(new Update(ns1App, meta),
                    new Update(ns2App, meta)), MutationOptions.DEFAULT);

    assertInOrder(mds, SearchRequest.of("*").setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC)).build(),
                  app1Record, app2Record);

    // clean up
    mds.batch(batch(new Drop(ns1App), new Drop(ns2App)), MutationOptions.DEFAULT);
  }

  @Test
  public void testSearchPagination() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns = "ns";
    MetadataEntity app1 = ofApp(ns, "app");
    MetadataEntity app2 = ofApp(ns, "app2");
    MetadataEntity service = ofService(app1, "service");
    MetadataEntity worker = ofWorker(app2, "worker");
    MetadataEntity dataset = ofDataset(ns, "dataset");
    MetadataEntity hidden = ofDataset(ns, "_auditLog");

    MetadataRecord serviceRecord = new MetadataRecord(service, new Metadata(USER, tags("tag", "tag1")));
    MetadataRecord workerRecord = new MetadataRecord(worker, new Metadata(USER, tags("tag2", "tag3 tag4")));
    MetadataRecord datasetRecord = new MetadataRecord(dataset, new Metadata(USER, tags("tag5 tag6", "tag7 tag8")));
    MetadataRecord hiddenRecord = new MetadataRecord(hidden, new Metadata(USER, tags("tag9", "tag10", "tag11",
                                                                                     "tag12", "tag13")));
    mds.batch(batch(new Update(service, serviceRecord.getMetadata()),
                    new Update(worker, workerRecord.getMetadata()),
                    new Update(dataset, datasetRecord.getMetadata()),
                    new Update(hidden, hiddenRecord.getMetadata())), MutationOptions.DEFAULT);

    // assert that search returns all records and determine the order of relevance )it varies by implementation)
    SearchResponse response = mds.search(
      SearchRequest.of("tag*").addNamespace(ns).setShowHidden(true).setLimit(Integer.MAX_VALUE).build());
    List<MetadataRecord> results = response.getResults();
    List<MetadataRecord> noHidden = results.stream()
      .filter(record -> !record.equals(hiddenRecord)).collect(Collectors.toList());
    Assert.assertEquals(ImmutableSet.of(serviceRecord, workerRecord, datasetRecord, hiddenRecord),
                        ImmutableSet.copyOf(results));

    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns).setLimit(Integer.MAX_VALUE).build(),
                  noHidden.get(0), noHidden.get(1), noHidden.get(2));

    // hidden entity should now be returned since showHidden is true
    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns).setShowHidden(true).build(),
                  results.get(0), results.get(1), results.get(2), results.get(3));

    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns).setLimit(2).setCursorRequested(true).build(),
                  noHidden.get(0), noHidden.get(1));

    // skipping hidden entity should not affect the offset
    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns).setOffset(1).setLimit(2).build(),
                  noHidden.get(1), noHidden.get(2));

    // if showHidden is true, the hidden entity should affect the offset
    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns).setOffset(1).setLimit(3).setShowHidden(true).build(),
                  results.get(1), results.get(2), results.get(3));
    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns).setOffset(2).setLimit(2).build(),
                  noHidden.get(2));
    assertEmpty(mds, SearchRequest.of("tag*").addNamespace(ns).setOffset(4).setLimit(2).build());
    assertResults(mds, SearchRequest.of("tag*").addNamespace(ns).setOffset(1).build(),
                  noHidden.get(1), noHidden.get(2));

    // clean up
    mds.batch(batch(new Drop(service), new Drop(worker), new Drop(dataset), new Drop(hidden)), MutationOptions.DEFAULT);
  }

  @Test
  public void testSortedSearchAndPagination() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    // create 10 unique random entity ids with random creation times
    NoDupRandom random = new NoDupRandom();
    List<MetadataEntity> entities = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      entities.add(ofDataset("myns", "ds" + String.valueOf(random.nextInt(1000))));
    }

    long creationTime = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(60);
    List<MetadataRecord> records = entities.stream().map(
      entity -> new MetadataRecord(
        entity, new Metadata(SYSTEM, props(MetadataConstants.CREATION_TIME_KEY,
                                           String.valueOf(creationTime + random.nextInt(1000000)),
                                           MetadataConstants.ENTITY_NAME_KEY,
                                           entity.getValue(entity.getType())))))
      .collect(Collectors.toList());

    // index all entities
    mds.batch(records.stream().map(
      record -> new MetadataMutation.Update(record.getEntity(), record.getMetadata()))
                .collect(Collectors.toList()), MutationOptions.DEFAULT);

    testSortedSearch(mds, records, ENTITY_NAME_KEY);
    testSortedSearch(mds, records, CREATION_TIME_KEY);

    // clean up
    mds.batch(entities.stream().map(Drop::new).collect(Collectors.toList()), MutationOptions.DEFAULT);
  }

  private void testSortedSearch(MetadataStorage mds, List<MetadataRecord> records, String sortKey) throws IOException {
    // test search by sort key itself
    for (MetadataRecord record : records) {
      assertResults(mds, SearchRequest.of(
        sortKey + ":" + record.getMetadata().getProperties(SYSTEM).get(sortKey)).build(), record);
    }

    Sorting asc = new Sorting(MetadataConstants.CREATION_TIME_KEY, Sorting.Order.ASC);
    Sorting desc = new Sorting(MetadataConstants.CREATION_TIME_KEY, Sorting.Order.DESC);
    List<MetadataRecord> sorted = new ArrayList<>(records);
    sorted.sort(Comparator.comparingLong(
      r -> Long.parseLong(r.getMetadata().getProperties().get(new ScopedName(SYSTEM, CREATION_TIME_KEY)))));
    List<MetadataRecord> reverse = new ArrayList<>(sorted);
    Collections.reverse(reverse);

    testSortedSearch(mds, asc, sorted);
    testSortedSearch(mds, desc, reverse);
  }

  private void testSortedSearch(MetadataStorage mds, Sorting sorting, List<MetadataRecord> sorted) throws IOException {
    // search once with sort by creation time and validate that records are in order
    SearchResponse response = mds.search(SearchRequest.of("*").setSorting(sorting).build());
    Assert.assertEquals(sorted, response.getResults());

    // search for a window of results
    assertInOrder(mds, SearchRequest.of("*").setSorting(sorting).setOffset(0).setLimit(4).build(),
                  sorted.subList(0, 4));
    assertInOrder(mds, SearchRequest.of("*").setSorting(sorting).setOffset(2).setLimit(4).build(),
                  sorted.subList(2, 6));
    assertInOrder(mds, SearchRequest.of("*").setSorting(sorting).setOffset(8).setLimit(4).build(),
                  sorted.subList(8, 10));
    assertEmpty(mds, SearchRequest.of("*").setSorting(sorting).setOffset(12).setLimit(4).build());

    // search with cursor
    response = assertInOrder(mds, SearchRequest.of("*").setSorting(sorting).setLimit(4)
                               .setCursorRequested(true).build(),
                             sorted.subList(0, 4));
    Assert.assertNotNull(response.getCursor());
    response = assertInOrder(mds, SearchRequest.of("*").setSorting(sorting).setLimit(4)
                               .setCursorRequested(true).setCursor(response.getCursor()).build(),
                             sorted.subList(4, 8));
    Assert.assertNotNull(response.getCursor());
    response = assertInOrder(mds, SearchRequest.of("*").setSorting(sorting).setLimit(4)
                               .setCursorRequested(true).setCursor(response.getCursor()).build(),
                             sorted.subList(8, 10));
    Assert.assertNull(response.getCursor());
  }

  @Test
  public void testCursorsOffsetsAndTotals() throws IOException {
    MetadataStorage mds = getMetadataStorage();
    List<MetadataRecord> records = IntStream.range(0, 20)
      .mapToObj(i -> new MetadataRecord(ofDataset(DEFAULT_NAMESPACE, "ds" + i),
                                        new Metadata(SYSTEM, props(ENTITY_NAME_KEY, "ds" + i))))
      .collect(Collectors.toList());
    mds.batch(records.stream()
                .map(record -> new Update(record.getEntity(), record.getMetadata()))
                .collect(Collectors.toList()), MutationOptions.DEFAULT);

    // no cursors
    validateCursorAndOffset(mds, 0, 10, null, false, 10, 0, 10, true, false);
    validateCursorAndOffset(mds, 5, 10, null, false, 10, 5, 10, true, false);
    validateCursorAndOffset(mds, 10, 10, null, false, 10, 10, 10, false, false);
    validateCursorAndOffset(mds, 15, 10, null, false, 5, 15, 10, false, false);
    validateCursorAndOffset(mds, 20, 10, null, false, 0, 20, 10, false, false);
    validateCursorAndOffset(mds, 25, 10, null, false, 0, 25, 10, false, false);

    // request cursors, but don't use them
    validateCursorAndOffset(mds, 0, 10, null, true, 10, 0, 10, true, true);
    validateCursorAndOffset(mds, 0, 20, null, true, 20, 0, 20, false, false);
    validateCursorAndOffset(mds, 0, 30, null, true, 20, 0, 30, false, false);

    // test that passing in an empty string as the cursor has the same effect as null
    validateCursorAndOffset(mds, 0, 10, "", true, 10, 0, 10, true, true);
    validateCursorAndOffset(mds, 0, 20, "", true, 20, 0, 20, false, false);
    validateCursorAndOffset(mds, 0, 30, "", true, 20, 0, 30, false, false);

    // request cursor, and use it
    String cursor = validateCursorAndOffset(mds, 0, 8, null, true, 8, 0, 8, true, true);
    cursor = validateCursorAndOffset(mds, 0, 8, cursor, true, 8, 8, 8, true, true);
    validateCursorAndOffset(mds, 0, 8, cursor, true, 4, 16, 8, false, false);

    // request a cursor that matches evenly with the number of results
    cursor = validateCursorAndOffset(mds, 0, 10, null, true, 10, 0, 10, true, true);
    validateCursorAndOffset(mds, 0, 10, cursor, true, 10, 10, 10, false, false);

    // ensure that offset and limit are superseded by cursor
    cursor = validateCursorAndOffset(mds, 0, 4, null, true, 4, 0, 4, true, true);
    cursor = validateCursorAndOffset(mds, 0, 0, cursor, true, 4, 4, 4, true, true);
    cursor = validateCursorAndOffset(mds, 10, 100, cursor, true, 4, 8, 4, true, true);
    cursor = validateCursorAndOffset(mds, 12, 2, cursor, true, 4, 12, 4, true, true);
    validateCursorAndOffset(mds, 1, 1, cursor, true, 4, 16, 4, false, false);

    // ensure that we can start searching without cursor, with offset, and request a cursor
    // whether a cursor is returned, is implementation dependent
    cursor = validateCursorAndOffset(mds, 4, 4, null, true, 4, 4, 4, true, null);
    validateCursorAndOffset(mds, 8, 4, cursor, true, 4, 8, 4, true, null);

    // clean up
    mds.batch(records.stream().map(MetadataRecord::getEntity).map(Drop::new).collect(Collectors.toList()),
              MutationOptions.DEFAULT);
  }

  @Nullable
  private String validateCursorAndOffset(MetadataStorage mds,
                                         int offset, int limit, String cursor, boolean requestCursor,
                                         int expectedResults, int expectedOffset, int expectedLimit,
                                         boolean expectMore, Boolean expectCursor)
    throws IOException {
    SearchResponse response = mds.search(SearchRequest.of("*")
                                           .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC))
                                           .setOffset(offset).setLimit(limit)
                                           .setCursor(cursor).setCursorRequested(requestCursor)
                                           .build());
    Assert.assertEquals(expectedResults, response.getResults().size());
    Assert.assertEquals(expectedOffset, response.getOffset());
    Assert.assertEquals(expectedLimit, response.getLimit());
    Assert.assertEquals(expectMore, response.getTotalResults() > response.getOffset() + response.getResults().size());
    if (expectCursor != null) {
      Assert.assertEquals(expectCursor, null != response.getCursor());
      if (expectCursor) {
        validateCursor(response.getCursor(), expectedOffset + expectedLimit, expectedLimit);
      }
    }
    return response.getCursor();
  }

  /**
   * Subclasses must override this with implementation-specific code
   * to validate that the new cursor reflects the correct offset and page size:
   *
   * @param expectedOffset the expected offset of the cursor
   * @param expectedPageSize the expected pagesize/limit of the cursor
   */
  protected abstract void validateCursor(String cursor, int expectedOffset, int expectedPageSize);

  @Test
  public void testConcurrency() throws IOException {
    int numThreads = 10;
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CompletionService<MetadataChange> completionService = new ExecutorCompletionService<>(executor);
    MetadataStorage mds = getMetadataStorage();
    MetadataEntity entity = MetadataEntity.ofDataset("myds");
    // add "r<i>" tags to be removed by the individual threads
    mds.apply(new Update(entity, new Metadata(
      USER, IntStream.range(0, numThreads).mapToObj(i -> "r" + i).collect(Collectors.toSet()))),
              MutationOptions.DEFAULT);
    // create normally replaces. Add directives to preserve all tags. Also create the expected tags
    Set<String> expectedTags = new HashSet<>();
    Map<ScopedNameOfKind, MetadataDirective> directives = new HashMap<>();
    IntStream.range(0, numThreads).forEach(i -> {
      directives.put(new ScopedNameOfKind(TAG, USER, "t" + i), MetadataDirective.KEEP);
      directives.put(new ScopedNameOfKind(TAG, USER, "r" + i), MetadataDirective.KEEP);
      directives.put(new ScopedNameOfKind(TAG, USER, "c" + i), MetadataDirective.KEEP);
      // create threads will add c<i> and update threads will add t<i>; all r<i> will be removed
      expectedTags.add("t" + i);
      expectedTags.add("c" + i);
    });
    try {
      // create conflicting threads that perform create, update, and remove on the same entity
      IntStream.range(0, numThreads).forEach(
        i -> {
          completionService.submit(
            () -> mds.apply(new Create(entity, new Metadata(USER, tags("c" + i)), directives),
                            MutationOptions.DEFAULT));
          completionService.submit(
            () -> mds.apply(new Update(entity, new Metadata(USER, tags("t" + i))),
                            MutationOptions.DEFAULT));
          completionService.submit(
            () -> mds.apply(new Remove(entity, Collections.singleton(new ScopedNameOfKind(TAG, USER, "r" + i))),
                            MutationOptions.DEFAULT));
        });
      IntStream.range(0, numThreads * 3).forEach(i -> {
        try {
          completionService.take();
        } catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
      });
      // validate that all "r" tags were removed and all "c" and "t" tags were added
      Assert.assertEquals(expectedTags, mds.read(new Read(entity)).getTags(USER));
    } finally {
      // clean up
      mds.apply(new Drop(entity), MutationOptions.DEFAULT);
    }
  }

  @Test
  public void testBatchConcurrency() throws IOException {
    int numThreads = 10; // T threads
    int numEntities = 20; // N entities
    MetadataStorage mds = getMetadataStorage();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CompletionService<List<MetadataChange>> completionService = new ExecutorCompletionService<>(executor);
    // Set up N entities with T tags (one to be removed per thread)
    Set<String> allRTags = IntStream.range(0, numThreads).mapToObj(t -> "r" + t).collect(Collectors.toSet());
    Map<Integer, MetadataEntity> entities = IntStream.range(0, numEntities)
      .boxed().collect(Collectors.toMap(i -> i, i -> MetadataEntity.ofDataset("myds" + i)));
    mds.batch(entities.values().stream()
                .map(entity -> new Update(entity, new Metadata(USER, allRTags))).collect(Collectors.toList()),
              MutationOptions.DEFAULT);

    // Generate a random but conflicting set of batches of mutations, one for each thread.
    // Construct the expected results for each entity along with the mutations
    // Also, because some threads will perform a Create, create a set of directives to preserve all other tags
    Map<Integer, Set<String>> expected = IntStream.range(0, numEntities)
      .boxed().collect(Collectors.toMap(i -> i, i -> new HashSet<>(allRTags)));
    Map<Integer, List<MetadataMutation>> mutations = IntStream.range(0, numThreads)
      .boxed().collect(Collectors.toMap(i -> i, i -> new ArrayList<>()));
    Map<ScopedNameOfKind, MetadataDirective> directives = new HashMap<>();
    Random rand = new Random(System.currentTimeMillis());
    IntStream.range(0, numThreads).forEach(t -> {
      directives.put(new ScopedNameOfKind(TAG, USER, "t" + t), MetadataDirective.KEEP);
      directives.put(new ScopedNameOfKind(TAG, USER, "r" + t), MetadataDirective.KEEP);
      directives.put(new ScopedNameOfKind(TAG, USER, "c" + t), MetadataDirective.KEEP);
      IntStream.range(0, numEntities).forEach(e -> {
        int random = rand.nextInt(100);
        if (random < 30) {
          expected.get(e).add("t" + t);
          mutations.get(t).add(new Update(entities.get(e), new Metadata(USER, tags("t" + t))));
        } else if (random < 60) {
          expected.get(e).add("c" + t);
          mutations.get(t).add(new Create(entities.get(e), new Metadata(USER, tags("c" + t)), directives));
        } else if (random < 90) {
          expected.get(e).remove("r" + t);
          mutations.get(t).add(new Remove(entities.get(e),
                                          Collections.singleton(new ScopedNameOfKind(TAG, USER, "r" + t))));
        }
      });
    });
    // submit all tasks and wait for their completion
    IntStream.range(0, numThreads).forEach(t -> completionService.submit(
      () -> mds.batch(mutations.get(t), MutationOptions.DEFAULT)));
    IntStream.range(0, numThreads).forEach(t -> {
      try {
        completionService.take();
      } catch (InterruptedException e) {
        throw Throwables.propagate(e);
      }
    });
    // validate that all "r" tags were removed and all "c" and "t" tags were added
    IntStream.range(0, numEntities).forEach(
      e -> {
        try {
          Assert.assertEquals("For entity " + entities.get(e), expected.get(e),
                              mds.read(new Read(entities.get(e))).getTags(USER));
        } catch (Exception ex) {
          throw Throwables.propagate(ex);
        }
      });
    // clean up
    mds.batch(entities.values().stream().map(Drop::new).collect(Collectors.toList()), MutationOptions.DEFAULT);
  }

  /**
   * It's not trivial to test conflicts between updates and drops, because the outcome is not
   * deterministic. Here we start with an entity that has one tag "a". We issue a Drop and an
   * Update to add tag "b" concurrently. After both operations complete (possibly with retry
   * after conflict), only two cases are possible:
   *
   * 1. The delete completes first: Tag "a" is gone and tag "b" is the only metadata.
   *
   * 2. The update completes first: Tag "b" is added and deleted along with "a" right after,
   *    and the metadata is empty.
   *
   * Because the race between the two mutations does not always happen, we run this 10 times.
   */
  @Test
  @Category(SlowTests.class)
  public void testUpdateDropConflict() throws IOException {
    MetadataEntity entity = MetadataEntity.ofDataset("myds");
    MetadataStorage mds = getMetadataStorage();
    int numTests = 10;
    IntStream.range(0, numTests).forEach(x -> {
      try {
        mds.apply(new Create(entity, new Metadata(USER, tags("a")), Collections.emptyMap()),
                  MutationOptions.DEFAULT);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CompletionService<MetadataChange> completionService = new ExecutorCompletionService<>(executor);
        MetadataMutation update = new Update(entity, new Metadata(USER, tags("b")));
        MetadataMutation drop = new Drop(entity);
        completionService.submit(() -> mds.apply(update, MutationOptions.DEFAULT));
        completionService.submit(() -> mds.apply(drop, MutationOptions.DEFAULT));
        completionService.take();
        completionService.take();
        // each entity is either dropped then updated (and then it has tag "b" only)
        // or it first update and then dropped (and then it has empty metadata)
        Assert.assertTrue(ImmutableSet.of(Metadata.EMPTY, new Metadata(USER, tags("b")))
                            .contains(mds.read(new Read(entity))));
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    });
    // clean up
    mds.apply(new Drop(entity), MutationOptions.DEFAULT);
  }

  /**
   * See {@link #testUpdateDropConflict()} for a description. The difference in this test is that
   * we issue batches of mutations over a collection of entities. The same assumptions apply,
   * however, for each entity.
   */
  @Test
  @Category(SlowTests.class)
  public void testUpdateDropConflictInBatch() throws IOException {
    int numTests = 10;
    int numThreads = 2;
    int numEntities = 20;
    MetadataStorage mds = getMetadataStorage();
    ExecutorService executor = Executors.newFixedThreadPool(numThreads);
    CompletionService<List<MetadataChange>> completionService = new ExecutorCompletionService<>(executor);
    Map<Integer, MetadataEntity> entities = IntStream.range(0, numEntities)
      .boxed().collect(Collectors.toMap(i -> i, i -> MetadataEntity.ofDataset("myds" + i)));
    List<MetadataMutation> creates = entities.values().stream()
      .map(e -> new Create(e, new Metadata(USER, tags("a")), Collections.emptyMap())).collect(Collectors.toList());
    Random rand = new Random(System.currentTimeMillis());
    IntStream.range(0, numTests).forEach(x -> {
      try {
        mds.batch(creates, MutationOptions.DEFAULT);
        Map<Integer, List<MetadataMutation>> mutations = IntStream.range(0, numThreads)
          .boxed().collect(Collectors.toMap(i -> i, i -> new ArrayList<>()));
        IntStream.range(0, numEntities).forEach(e -> {
          // ensure that at least one thread attempts to drop this entity
          int dropThread = rand.nextInt(numThreads);
          IntStream.range(0, numThreads).forEach(t -> {
            if (t == dropThread || rand.nextInt(100) < 50) {
              mutations.get(t).add(new Drop(entities.get(e)));
            } else {
              mutations.get(t).add(new Update(entities.get(e), new Metadata(USER, tags("b"))));
            }
          });
        });
        IntStream.range(0, numThreads).forEach(t -> completionService.submit(
          () -> mds.batch(mutations.get(t), MutationOptions.DEFAULT)));
        IntStream.range(0, numThreads).forEach(t -> {
          try {
            completionService.take();
          } catch (InterruptedException e) {
            throw Throwables.propagate(e);
          }
        });
        IntStream.range(0, numEntities).forEach(
          e -> {
            try {
              // each entity is either dropped then updated (and then it has tag "b" only)
              // or it first update and then dropped (and then it has empty metadata)
              Assert.assertTrue(ImmutableSet.of(Metadata.EMPTY, new Metadata(USER, tags("b")))
                                  .contains(mds.read(new Read(entities.get(e)))));
            } catch (Exception ex) {
              throw Throwables.propagate(ex);
            }
          });
      } catch (Exception e) {
        throw Throwables.propagate(e);
      }
    });
    mds.batch(entities.values().stream().map(Drop::new).collect(Collectors.toList()), MutationOptions.DEFAULT);
  }

  private static class NoDupRandom {
    private final Random random = new Random(System.currentTimeMillis());
    private final Set<Integer> seen = new HashSet<>();

    int nextInt(int bound) {
      while (true) {
        int value = random.nextInt(bound);
        if (!seen.contains(value)) {
          seen.add(value);
          return value;
        }
      }
    }
  }

  private void verifyMetadataSelection(MetadataStorage mds, MetadataEntity entity, Metadata metadata,
                                       ImmutableSet<ScopedNameOfKind> selection) throws IOException {
    Assert.assertEquals(filterBy(metadata, selection), mds.read(new Read(entity, selection)));
  }

  private void verifyMetadata(MetadataStorage mds, MetadataEntity entity, Metadata metadata) throws IOException {
    // verify entire metadata
    Assert.assertEquals(metadata, mds.read(new Read(entity)));

    // filter by scope
    verifyFilteredMetadata(mds, entity, metadata, SYSTEM, null);
    verifyFilteredMetadata(mds, entity, metadata, USER, null);

    // filter by kind
    verifyFilteredMetadata(mds, entity, metadata, null, PROPERTY);
    verifyFilteredMetadata(mds, entity, metadata, null, MetadataKind.TAG);

    // filter by kind and scope
    verifyFilteredMetadata(mds, entity, metadata, SYSTEM, PROPERTY);
    verifyFilteredMetadata(mds, entity, metadata, SYSTEM, MetadataKind.TAG);
    verifyFilteredMetadata(mds, entity, metadata, USER, PROPERTY);
    verifyFilteredMetadata(mds, entity, metadata, USER, MetadataKind.TAG);
  }

  private void verifyFilteredMetadata(MetadataStorage mds, MetadataEntity entity, Metadata metadata,
                                      MetadataScope scope, MetadataKind kind) throws IOException {
    Assert.assertEquals(filterBy(metadata, scope, kind), mds.read(new Read(entity, scope, kind)));
  }

  private void verifyMetadataSelectionAsync(MetadataStorage mds, MetadataEntity entity, Metadata metadata,
                                            ImmutableSet<ScopedNameOfKind> selection) throws Exception {
    Tasks.waitFor(true, () -> mds.read(new Read(entity, selection)).equals(filterBy(metadata, selection)),
                  10, TimeUnit.SECONDS);
  }

  private void verifyMetadataAsync(MetadataStorage mds, MetadataEntity entity, Metadata metadata) throws Exception {
    // Wait 10 Seconds for entity's entity metadata to reflect the Mutation
    Tasks.waitFor(true, () -> mds.read(new Read(entity)).equals(metadata), 10, TimeUnit.SECONDS);

    // filter by scope
    verifyFilteredMetadata(mds, entity, metadata, SYSTEM, null);
    verifyFilteredMetadata(mds, entity, metadata, USER, null);

    // filter by kind
    verifyFilteredMetadata(mds, entity, metadata, null, PROPERTY);
    verifyFilteredMetadata(mds, entity, metadata, null, MetadataKind.TAG);

    // filter by kind and scope
    verifyFilteredMetadata(mds, entity, metadata, SYSTEM, PROPERTY);
    verifyFilteredMetadata(mds, entity, metadata, SYSTEM, MetadataKind.TAG);
    verifyFilteredMetadata(mds, entity, metadata, USER, PROPERTY);
    verifyFilteredMetadata(mds, entity, metadata, USER, MetadataKind.TAG);
  }

//  private void verifyFilteredMetadataAsync(MetadataStorage mds, MetadataEntity entity, Metadata metadata,
//                                           MetadataScope scope, MetadataKind kind) throws Exception {
//    Tasks.waitFor(true,
//                  () -> mds.read(new Read(entity, scope, kind)).equals(filterBy(metadata, scope, kind)),
//                  10, TimeUnit.SECONDS);
//  }

  private Metadata filterBy(Metadata metadata, MetadataScope scope, MetadataKind kind) {
    //noinspection ConstantConditions
    return new Metadata(
      kind == PROPERTY ? Collections.emptySet()
        : Sets.filter(metadata.getTags(), tag -> scope == null || scope.equals(tag.getScope())),
      kind == MetadataKind.TAG ? Collections.EMPTY_MAP
        : Maps.filterKeys(metadata.getProperties(), key -> scope == null || scope.equals(key.getScope())));
  }

  private Metadata filterBy(Metadata metadata, ImmutableSet<ScopedNameOfKind> selection) {
    //noinspection ConstantConditions
    return new Metadata(
      Sets.filter(metadata.getTags(), tag ->
        selection.contains(new ScopedNameOfKind(MetadataKind.TAG, tag.getScope(), tag.getName()))),
      Maps.filterKeys(metadata.getProperties(), key ->
        selection.contains(new ScopedNameOfKind(PROPERTY, key.getScope(), key.getName()))));
  }


  private void assertEmpty(MetadataStorage mds, SearchRequest request) throws IOException {
    assertInOrder(mds, request, Collections.emptyList());
  }

  @SuppressWarnings("WeakerAccess")
  protected static SearchResponse assertInOrder(MetadataStorage mds, SearchRequest request,
                                                List<MetadataRecord> expectedResults)
    throws IOException {
    SearchResponse response = mds.search(request);
    Assert.assertEquals("for query '" + request.getQuery() + "':", expectedResults, response.getResults());
    return response;
  }

  @SuppressWarnings("UnusedReturnValue")
  protected static SearchResponse assertInOrder(MetadataStorage mds, SearchRequest request,
                                                MetadataRecord... expectedResults)
    throws IOException {
    return assertInOrder(mds, request, ImmutableList.copyOf(expectedResults));
  }

  protected static SearchResponse assertResults(MetadataStorage mds, SearchRequest request,
                                                MetadataRecord firstResult, MetadataRecord... expectedResults)
    throws IOException {
    SearchResponse response = mds.search(request);
    Assert.assertEquals("for query '" + request.getQuery() + "':",
                        ImmutableSet.<MetadataRecord>builder().add(firstResult).add(expectedResults).build(),
                        ImmutableSet.copyOf(response.getResults()));
    return response;
  }

  protected static Metadata union(Metadata meta, Metadata other) {
    return new Metadata(Sets.union(meta.getTags(), other.getTags()),
                        ImmutableMap.<ScopedName, String>builder()
                          .putAll(meta.getProperties())
                          .putAll(other.getProperties()).build());
  }

  @SafeVarargs
  protected static <T> Set<T> tags(T... elements) {
    return ImmutableSet.copyOf(elements);
  }

  protected static <K, V> Map<K, V> props() {
    return ImmutableMap.of();
  }
  protected static <K, V> Map<K, V> props(K k, V v) {
    return ImmutableMap.of(k, v);
  }
  protected static <K, V> Map<K, V> props(K k1, V v1, K k2, V v2) {
    return ImmutableMap.of(k1, v1, k2, v2);
  }
  protected static <K, V> Map<K, V>  props(K k1, V v1, K k2, V v2, K k3, V v3) {
    return ImmutableMap.of(k1, v1, k2, v2, k3, v3);
  }
  protected static <K, V> Map<K, V>  props(K k1, V v1, K k2, V v2, K k3, V v3, K k4, V v4) {
    return ImmutableMap.of(k1, v1, k2, v2, k3, v3, k4, v4);
  }

  private static MetadataEntity ofDataset(String ns, String dataset) {
    return MetadataEntity.ofDataset(ns, dataset);
  }

  @SuppressWarnings("SameParameterValue")
  private static MetadataEntity ofAppNoVersion(String ns, String app) {
    return MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, ns)
      .appendAsType(MetadataEntity.APPLICATION, app)
      .build();
  }

  private static MetadataEntity ofApp(String ns, String app) {
    return MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, ns)
      .appendAsType(MetadataEntity.APPLICATION, app)
      .append(MetadataEntity.VERSION, "-SNAPSHOT")
      .build();
  }

  @SuppressWarnings("SameParameterValue")
  private static MetadataEntity ofArtifact(String ns, String artifact, String version) {
    return MetadataEntity.builder()
      .append(MetadataEntity.NAMESPACE, ns)
      .appendAsType(MetadataEntity.ARTIFACT, artifact)
      .append(MetadataEntity.VERSION, version)
      .build();
  }

  private static MetadataEntity ofProgram(MetadataEntity app, String type, String program) {
    return MetadataEntity.builder(app)
      .append(MetadataEntity.TYPE, type)
      .appendAsType(MetadataEntity.PROGRAM, program)
      .build();
  }

  @SuppressWarnings("SameParameterValue")
  private static MetadataEntity ofService(MetadataEntity app, String service) {
    return ofProgram(app, "Service", service);
  }

  private static MetadataEntity ofWorker(MetadataEntity app, String worker) {
    return ofProgram(app, "Worker", worker);
  }

  @SuppressWarnings("SameParameterValue")
  private static MetadataEntity ofWorkflow(MetadataEntity app, String workflow) {
    return ofProgram(app, "Workflow", workflow);
  }

}
