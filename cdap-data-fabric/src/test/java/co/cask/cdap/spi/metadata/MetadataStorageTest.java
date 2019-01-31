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

package co.cask.cdap.spi.metadata;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
import co.cask.cdap.proto.element.EntityTypeSimpleName;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.spi.metadata.MetadataMutation.Drop;
import co.cask.cdap.spi.metadata.MetadataMutation.Remove;
import co.cask.cdap.spi.metadata.MetadataMutation.Update;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static co.cask.cdap.api.metadata.MetadataScope.SYSTEM;
import static co.cask.cdap.api.metadata.MetadataScope.USER;
import static co.cask.cdap.data2.metadata.system.SystemMetadataProvider.ENTITY_NAME_KEY;
import static co.cask.cdap.spi.metadata.MetadataKind.PROPERTY;
import static co.cask.cdap.spi.metadata.MetadataKind.TAG;

/**
 * Tests for Metadata SPI implementations.
 */
public abstract class MetadataStorageTest {

  private static final String TYPE_ARTIFACT = EntityTypeSimpleName.ARTIFACT.name();
  private static final String TYPE_DATASET = EntityTypeSimpleName.DATASET.name();
  private static final String TYPE_PROGRAM = EntityTypeSimpleName.PROGRAM.name();

  protected abstract MetadataStorage getMetadataStorage();

  @After
  public void ensureCleanUp() throws Exception {
    assertEmpty(getMetadataStorage(), SearchRequest.of("*").setShowHidden(true).build());
  }

  // TODO (CDAP-14806): Add more tests for custom entities; refactor some of these tests to use custom entities.

  @Test
  public void testMutations() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity entity = NamespaceId.DEFAULT.dataset("entity").toMetadataEntity();

    // get metadata for non-existing entity
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // drop metadata for non-existing entity succeeds
    MetadataChange change = mds.apply(new Drop(entity));
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // remove metadata for non-existing entity succeeds
    mds.apply(new Remove(entity, ImmutableSet.of(
      new ScopedNameOfKind(MetadataKind.TAG, SYSTEM, "st1"),
      new ScopedNameOfKind(MetadataKind.TAG, USER,   "ut1"),
      new ScopedNameOfKind(PROPERTY, SYSTEM, "sp2"),
      new ScopedNameOfKind(PROPERTY, USER,   "up2"))));
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // update metadata for non-existing entity creates it
    Metadata metadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "a"),
                      new ScopedName(USER,   "b")),
      ImmutableMap.of(new ScopedName(SYSTEM, "p"), "v",
                      new ScopedName(USER,   "k"), "v1"));
    change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // test that update is idempotent
    change = mds.apply(new Update(entity, metadata));
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
    MetadataMutation create = new MetadataMutation.Create(entity, metadata, Collections.emptyMap());
    change = mds.apply(create);
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
    MetadataMutation recreate = new MetadataMutation.Create(entity, recreatedMetadata, ImmutableMap.of(
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
    change = mds.apply(recreate);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // replace the metadata with directives
    recreatedMetadata = new Metadata(
      ImmutableSet.of(new ScopedName(SYSTEM, "nst1"),
                      new ScopedName(USER,   "nut1")),
      ImmutableMap.of(new ScopedName(SYSTEM, "sp1"),  "nsv1",
                      new ScopedName(SYSTEM, "nsp2"), "sv2",
                      new ScopedName(USER,   "up3"),  "uv3"));
    recreate = new MetadataMutation.Create(entity, recreatedMetadata, ImmutableMap.of(
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
    change = mds.apply(recreate);
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
    change = mds.apply(update);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // test that update is idempotent
    change = mds.apply(update);
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
    change = mds.apply(remove);
    Assert.assertEquals(new MetadataChange(entity, previousMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // test that remove is idemtpotent
    change = mds.apply(remove);
    Assert.assertEquals(new MetadataChange(entity, metadata, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // drop all metadata for the entity
    change = mds.apply(new Drop(entity));
    Assert.assertEquals(new MetadataChange(entity, metadata, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // drop is idempotent
    change = mds.apply(new Drop(entity));
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);
  }

  @Test
  public void testUpdateRemove() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity entity = NamespaceId.DEFAULT.dataset("entity").toMetadataEntity();

    // get metadata for non-existing entity
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // add some metadata for the entity
    Metadata metadata = new Metadata(ImmutableSet.of(new ScopedName(SYSTEM, "sysTag"),
                                                     new ScopedName(USER, "userTag")),
                                     ImmutableMap.of(new ScopedName(SYSTEM, "sysProp"), "sysVal",
                                                     new ScopedName(USER, "userProp"), "userVal"));
    MetadataChange change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, metadata), change);
    verifyMetadata(mds, entity, metadata);

    // remove everything
    change = mds.apply(new Remove(entity));
    Assert.assertEquals(new MetadataChange(entity, metadata, Metadata.EMPTY), change);
    verifyMetadata(mds, entity, Metadata.EMPTY);

    // add back all metadata, then remove everything in user scope
    change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, Metadata.EMPTY, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, USER));
    Metadata newMetadata = filterBy(metadata, SYSTEM, null);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove everything in system scope
    change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, SYSTEM));
    newMetadata = filterBy(metadata, USER, null);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all tags
    change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, TAG));
    newMetadata = filterBy(metadata, null, PROPERTY);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all properties
    change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, PROPERTY));
    newMetadata = filterBy(metadata, null, TAG);
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all properties in system scope
    change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, SYSTEM, PROPERTY));
    newMetadata = union(filterBy(metadata, SYSTEM, TAG), filterBy(metadata, USER, null));
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // add back all metadata, then remove all tags in user scope
    change = mds.apply(new Update(entity, metadata));
    Assert.assertEquals(new MetadataChange(entity, newMetadata, metadata), change);
    verifyMetadata(mds, entity, metadata);
    change = mds.apply(new Remove(entity, USER, TAG));
    newMetadata = union(filterBy(metadata, USER, PROPERTY), filterBy(metadata, SYSTEM, null));
    Assert.assertEquals(new MetadataChange(entity, metadata, newMetadata), change);
    verifyMetadata(mds, entity, newMetadata);

    // clean up
    mds.apply(new Drop(entity));
  }

  @Test
  public void testSearchOnTags() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    final String ns1 = "ns1";
    final String ns2 = "ns2";
    final NamespaceId ns1Id = new NamespaceId(ns1);
    final NamespaceId ns2Id = new NamespaceId(ns2);
    final ApplicationId app1Id = ns1Id.app("app1");
    final ApplicationId app2Id = ns2Id.app("app1");
    final MetadataEntity app1 = app1Id.toMetadataEntity();
    final MetadataEntity app2 = app2Id.toMetadataEntity();
    final MetadataEntity program1 = app1Id.worker("wk1").toMetadataEntity();
    final MetadataEntity dataset1 = ns1Id.dataset("ds1").toMetadataEntity();
    final MetadataEntity dataset2 = ns1Id.dataset("ds2").toMetadataEntity();
    final MetadataEntity file1 = MetadataEntity.builder(dataset1).appendAsType("file", "f1").build();

    List<MetadataEntity> entities = ImmutableList.of(app1, app2, program1, dataset1, dataset2, file1);
    for (MetadataEntity entity : entities) {
      Assert.assertEquals(Metadata.EMPTY, mds.read(new Read(entity)));
    }

    // add tags for these entities
    MetadataRecord
      app1Record = new MetadataRecord(app1, new Metadata(USER, tags("tag1", "tag2", "tag3"))),
      app2Record = new MetadataRecord(app2, new Metadata(USER, tags("tag1", "tag2", "tag3_more"))),
      program1Record = new MetadataRecord(program1, new Metadata(USER, tags("tag1"))),
      dataset1Record = new MetadataRecord(dataset1, new Metadata(USER, tags("tag3", "tag2", "tag12-tag33"))),
      dataset2Record = new MetadataRecord(dataset2, new Metadata(USER, tags("tag2", "tag4"))),
      file1Record = new MetadataRecord(file1, new Metadata(USER, tags("tag2", "tag5")));

    mds.batch(ImmutableList.of(app1Record, app2Record, program1Record, dataset1Record, dataset2Record, file1Record)
                .stream().map(record -> new Update(record.getEntity(), record.getMetadata()))
                .collect(Collectors.toList()));

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
    mds.batch(entities.stream().map(Drop::new).collect(Collectors.toList()));

    // Search should be empty after deleting tags
    assertEmpty(mds, SearchRequest.of("*").setLimit(10).build());
  }

  @Test
  public void testSearchOnTagsUpdate() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity entity = NamespaceId.DEFAULT.app("appX").workflow("wtf").toMetadataEntity();
    Metadata meta = new Metadata(SYSTEM, tags("tag1", "tag2"));
    mds.apply(new Update(entity, meta));
    Assert.assertEquals(meta.getTags(SYSTEM), mds.read(new Read(entity, SYSTEM)).getTags(SYSTEM));
    assertResults(mds, SearchRequest.of("tag1").build(), new MetadataRecord(entity, meta));

    // add an more tags
    mds.apply(new Update(entity, new Metadata(SYSTEM, tags("tag3", "tag4"))));
    Set<String> newTags = tags("tag1", "tag2", "tag3", "tag4");
    Metadata newMeta = new Metadata(SYSTEM, newTags);
    Assert.assertEquals(newTags, mds.read(new Read(entity, SYSTEM)).getTags(SYSTEM));
    for (String expectedTag : newTags) {
      assertResults(mds, SearchRequest.of(expectedTag).build(), new MetadataRecord(entity, newMeta));
    }

    // add an empty set of tags. This should have no effect on retrieval or search of tags
    mds.apply(new Update(entity, new Metadata(SYSTEM, tags())));
    Assert.assertEquals(newTags, mds.read(new Read(entity, SYSTEM)).getTags(SYSTEM));
    for (String expectedTag : newTags) {
      assertResults(mds, SearchRequest.of(expectedTag).build(), new MetadataRecord(entity, newMeta));
    }

    // clean up
    mds.apply(new Drop(entity));
  }

  // TODO: add tests that search by specific fields: namespace, entity name, schema, creation time,

  @Test
  public void testSearchOnTypes() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity myDs = NamespaceId.DEFAULT.dataset("myDs").toMetadataEntity();
    MetadataEntity myField1 = MetadataEntity.builder(myDs).appendAsType("field", "myField1").build();
    MetadataEntity myField2 = MetadataEntity.builder(myDs).appendAsType("field", "myField2").build();
    MetadataRecord record1 = new MetadataRecord(myField1, new Metadata(USER, props("testKey1", "testValue1")));
    MetadataRecord record2 = new MetadataRecord(myField2, new Metadata(USER, props("testKey2", "testValue2")));

    mds.batch(batch(new Update(myField1, record1.getMetadata()), new Update(myField2, record2.getMetadata())));

    // Search for it based on value
    assertResults(mds, SearchRequest.of("field:myField1").build(), record1);

    // should return both fields
    assertResults(mds, SearchRequest.of("field:myFie*").build(), record1, record2);
    assertResults(mds, SearchRequest.of("field*").build(), record1, record2);

    // clean up
    mds.batch(batch(new Drop(myField1), new Drop(myField2)));
  }

  @Test
  public void testSearchOnValue() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    final NamespaceId nsId = new NamespaceId("ns1");
    final ApplicationId appId = nsId.app("app1");
    final MetadataEntity program = appId.worker("wk1").toMetadataEntity();
    final MetadataEntity dataset = nsId.dataset("ds2").toMetadataEntity();

    // Add some metadata
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    MetadataRecord programRecord = new MetadataRecord(
      program, new Metadata(USER, props("key1", "value1", "key2", "value2", "multiword", multiWordValue)));
    mds.apply(new Update(program, programRecord.getMetadata()));

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
    mds.apply(new Update(program, new Metadata(SYSTEM, props("key3", "value1"))));
    programRecord = new MetadataRecord(
      program, new Metadata(ImmutableSet.of(), ImmutableMap.of(new ScopedName(USER, "key1"), "value1",
                                                               new ScopedName(USER, "key2"), "value2",
                                                               new ScopedName(USER, "multiword"), multiWordValue,
                                                               new ScopedName(SYSTEM, "key3"), "value1")));
    // search by value
    assertResults(mds, SearchRequest.of("value1").addType(TYPE_PROGRAM).build(), programRecord);

    // add a property for the dataset
    MetadataRecord datasetRecord = new MetadataRecord(dataset, new Metadata(USER, props("key21", "value21")));
    mds.apply(new Update(dataset, datasetRecord.getMetadata()));

    // Search based on value prefix
    assertResults(mds, SearchRequest.of("value2*").build(), programRecord, datasetRecord);

    // Search based on value prefix in the wrong namespace
    assertEmpty(mds, SearchRequest.of("value2*").addNamespace("ns12").build());

    // clean up
    mds.batch(batch(new Drop(program), new Drop(dataset)));
  }

  @Test
  public void testSearchOnKeyValue() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    final NamespaceId nsId = new NamespaceId("ns1");
    final ApplicationId appId = nsId.app("app1");
    final MetadataEntity program = appId.worker("wk1").toMetadataEntity();
    final MetadataEntity dataset = nsId.dataset("ds2").toMetadataEntity();

    // add properties for program and dataset
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    MetadataRecord programRecord = new MetadataRecord(
      program, new Metadata(USER, props("key1", "value1", "key2", "value2", "multiword", multiWordValue)));
    MetadataRecord datasetRecord = new MetadataRecord(dataset,
      new Metadata(ImmutableSet.of(), ImmutableMap.of(new ScopedName(SYSTEM, "sKey1"), "sValue1",
                                                      new ScopedName(USER, "Key1"), "Value1")));

    mds.batch(batch(new Update(program, programRecord.getMetadata()),
                    new Update(dataset, datasetRecord.getMetadata())));

    // Search for it based on key and value
    assertResults(mds, SearchRequest.of("key1:value1").addType(TYPE_PROGRAM).build(), programRecord);
    assertResults(mds, SearchRequest.of("key1:value1").build(), programRecord, datasetRecord);

    // Search for it based on a word in value
    assertResults(mds, SearchRequest.of("multiword:aV5").build(), programRecord);

    // Search for it based on a word in value with spaces in search query
    assertResults(mds, SearchRequest.of("  multiword:aV1   ").build(), programRecord);

    // remove the multiword property
    mds.apply(new Remove(program, ImmutableSet.of(new ScopedNameOfKind(PROPERTY, USER, "multiword"))));
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
    mds.batch(batch(new Drop(program), new Drop(dataset)));
  }

  @Test
  public void testUpdateSearch() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    final String ns = "ns";
    final MetadataEntity program = new NamespaceId(ns).app("app1").worker("wk1").toMetadataEntity();

    Metadata meta = new Metadata(USER, tags("tag1", "tag2"), props("key1", "value1", "key2", "value2"));
    MetadataRecord programRecord = new MetadataRecord(program, meta);
    mds.apply(new Update(program, meta));

    assertResults(mds, SearchRequest.of("value1").addNamespace(ns).build(), programRecord);
    assertResults(mds, SearchRequest.of("value2").addNamespace(ns).build(), programRecord);
    assertResults(mds, SearchRequest.of("tag2").addNamespace(ns).build(), programRecord);

    mds.apply(new Update(program, new Metadata(USER, props("key1", "value3"))));
    mds.apply(new Remove(program, ImmutableSet.of(new ScopedNameOfKind(PROPERTY, USER, "key2"),
                                                  new ScopedNameOfKind(TAG, USER, "tag2"))));
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
    mds.apply(new Drop(program));
  }

  @Test
  public void testSearchIncludesSystemEntities() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    final String ns1 = "ns1";
    final String ns2 = "ns2";
    final NamespaceId ns1Id = new NamespaceId(ns1);
    final NamespaceId ns2Id = new NamespaceId(ns2);
    final ApplicationId appId = ns1Id.app("app1");
    final MetadataEntity program = appId.worker("wk1").toMetadataEntity();
    // Use the same artifact in two different namespaces - system and ns2
    final MetadataEntity artifact = ns2Id.artifact("artifact", "1.0").toMetadataEntity();
    final MetadataEntity sysArtifact = NamespaceId.SYSTEM.artifact("artifact", "1.0").toMetadataEntity();

    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Metadata meta = new Metadata(SYSTEM, props(multiWordKey, multiWordValue));

    MetadataRecord programRecord = new MetadataRecord(program, meta);
    MetadataRecord artifactRecord = new MetadataRecord(artifact, meta);
    MetadataRecord sysArtifactRecord = new MetadataRecord(sysArtifact, meta);

    mds.batch(batch(new Update(program, meta), new Update(artifact, meta), new Update(sysArtifact, meta)));

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
    mds.batch(batch(new Drop(program), new Drop(artifact), new Drop(sysArtifact)));
  }

  private List<? extends MetadataMutation> batch(MetadataMutation ... mutations) {
    return ImmutableList.copyOf(mutations);
  }

  @Test
  public void testSearchDifferentNamespaces() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    final String ns1 = "ns1";
    final NamespaceId ns1Id = new NamespaceId(ns1);
    final MetadataEntity artifact = ns1Id.artifact("artifact", "1.0").toMetadataEntity();
    final MetadataEntity sysArtifact = NamespaceId.SYSTEM.artifact("artifact", "1.0").toMetadataEntity();

    final String multiWordKey = "multiword";
    final String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Metadata meta = new Metadata(SYSTEM, props(multiWordKey, multiWordValue));

    MetadataRecord artifactRecord = new MetadataRecord(artifact, meta);
    MetadataRecord sysArtifactRecord = new MetadataRecord(sysArtifact, meta);

    mds.batch(batch(new Update(artifact, meta), new Update(sysArtifact, meta)));

    // searching only user namespace should not return system entity
    assertResults(mds, SearchRequest.of("aV5").addNamespace(ns1).build(), artifactRecord);

    // searching only user namespace and system should return only the system entity
    assertResults(mds, SearchRequest.of("aV5").addSystemNamespace().build(), sysArtifactRecord);

    // searching only user namespace and system should return both entities
    assertResults(mds, SearchRequest.of("aV5").addNamespace(ns1).addSystemNamespace().build(),
                  artifactRecord, sysArtifactRecord);

    // clean up
    mds.batch(batch(new Drop(artifact), new Drop(sysArtifact)));
  }

  @Test
  public void testCrossNamespaceSearch() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceId ns2 = new NamespaceId("ns2");

    MetadataEntity ns1app1 = ns1.app("a1").toMetadataEntity();
    MetadataEntity ns1app2 = ns1.app("a2").toMetadataEntity();
    MetadataEntity ns1app3 = ns1.app("a3").toMetadataEntity();
    MetadataEntity ns2app1 = ns2.app("a1").toMetadataEntity();
    MetadataEntity ns2app2 = ns2.app("a2").toMetadataEntity();

    MetadataRecord
      record11 = new MetadataRecord(ns1app1, new Metadata(USER, tags("v1"), props("k1", "v1"))),
      record12 = new MetadataRecord(ns1app2, new Metadata(USER, props("k1", "v1", "k2", "v2"))),
      record13 = new MetadataRecord(ns1app3, new Metadata(USER, props("k1", "v1", "k3", "v3"))),
      record21 = new MetadataRecord(ns2app1, new Metadata(USER, props("k1", "v1", "k2", "v2"))),
      record22 = new MetadataRecord(ns2app2, new Metadata(USER, tags("v2", "v3"), props("k1", "v1")));
    MetadataRecord[] records = { record11, record12, record13, record21, record22 };
    // apply all metadata in batch
    mds.batch(Arrays.stream(records)
                .map(record -> new Update(record.getEntity(), record.getMetadata())).collect(Collectors.toList()));

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
      new Drop(ns1app1), new Drop(ns1app2), new Drop(ns1app3), new Drop(ns2app1), new Drop(ns2app2)));
  }

  @Test
  public void testCrossNamespaceDefaultSearch() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    NamespaceId ns1 = new NamespaceId("ns1");
    NamespaceId ns2 = new NamespaceId("ns2");
    MetadataEntity ns1app = ns1.app("a").toMetadataEntity();
    MetadataEntity ns2app = ns2.app("a").toMetadataEntity();

    MetadataRecord app1Record = new MetadataRecord(ns1app, new Metadata(USER, props("k1", "v1", "k2", "v2")));
    MetadataRecord app2Record = new MetadataRecord(ns2app, new Metadata(USER, props("k1", "v1")));

    mds.batch(batch(new Update(ns1app, app1Record.getMetadata()),
                    new Update(ns2app, app2Record.getMetadata())));

    assertResults(mds, SearchRequest.of("v1").build(), app1Record, app2Record);
    assertResults(mds, SearchRequest.of("v2").build(), app1Record);
    assertResults(mds, SearchRequest.of("*").build(), app1Record, app2Record);

    // clean up
    mds.batch(batch(new Drop(ns1app), new Drop(ns2app)));
  }

  @Test
  public void testCrossNamespaceCustomSearch() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    String appName = "app";
    MetadataEntity ns1App = new NamespaceId("ns1").app(appName).toMetadataEntity();
    MetadataEntity ns2App = new NamespaceId("ns2").app(appName).toMetadataEntity();

    Metadata meta = new Metadata(SYSTEM, props(ENTITY_NAME_KEY, appName));
    MetadataRecord app1Record = new MetadataRecord(ns1App, meta);
    MetadataRecord app2Record = new MetadataRecord(ns2App, meta);

    mds.batch(batch(new Update(ns1App, meta),
                    new Update(ns2App, meta)));

    assertInOrder(mds, SearchRequest.of("*").setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC)).build(),
                  app1Record, app2Record);

    // clean up
    mds.batch(batch(new Drop(ns1App), new Drop(ns2App)));
  }

  @Test
  public void testSearchPagination() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns = "ns";
    NamespaceId nsId = new NamespaceId(ns);
    MetadataEntity service = nsId.app("app").service("service").toMetadataEntity();
    MetadataEntity worker = nsId.app("app2").worker("worker").toMetadataEntity();
    MetadataEntity dataset = nsId.dataset("dataset").toMetadataEntity();
    MetadataEntity hidden = nsId.dataset("_auditLog").toMetadataEntity();

    MetadataRecord serviceRecord = new MetadataRecord(service, new Metadata(USER, tags("tag", "tag1")));
    MetadataRecord workerRecord = new MetadataRecord(worker, new Metadata(USER, tags("tag2", "tag3 tag4")));
    MetadataRecord datasetRecord = new MetadataRecord(dataset, new Metadata(USER, tags("tag5 tag6", "tag7 tag8")));
    MetadataRecord hiddenRecord = new MetadataRecord(hidden, new Metadata(USER, tags("tag9", "tag10", "tag11",
                                                                                     "tag12", "tag13")));
    mds.batch(batch(new Update(service, serviceRecord.getMetadata()),
                    new Update(worker, workerRecord.getMetadata()),
                    new Update(dataset, datasetRecord.getMetadata()),
                    new Update(hidden, hiddenRecord.getMetadata())));

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
    mds.batch(batch(new Drop(service), new Drop(worker), new Drop(dataset), new Drop(hidden)));
  }

  @Test
  public void testPaginationWithSorting() throws Exception {

    final String ns = "ns1";
    final String programName = "prog";
    final String datasetName = "ds";
    final String appName = "app";
    final NamespaceId ns1Id = new NamespaceId(ns);
    final ApplicationId app1Id = ns1Id.app("app");
    final MetadataEntity app = app1Id.toMetadataEntity();
    final MetadataEntity program = app1Id.worker(programName).toMetadataEntity();
    final MetadataEntity dataset = ns1Id.dataset(datasetName).toMetadataEntity();

    MetadataRecord appRecord =
      new MetadataRecord(app, new Metadata(SYSTEM, props(ENTITY_NAME_KEY, appName, "k", "name1")));
    MetadataRecord datasetRecord =
      new MetadataRecord(dataset, new Metadata(SYSTEM, props(ENTITY_NAME_KEY, datasetName, "kk", "name2")));
    MetadataRecord programRecord =
      new MetadataRecord(program, new Metadata(SYSTEM, props(ENTITY_NAME_KEY, programName, "kkk", "name3")));

    MetadataStorage mds = getMetadataStorage();
    mds.batch(batch(new Update(app, appRecord.getMetadata()),
                    new Update(dataset, datasetRecord.getMetadata()),
                    new Update(program, programRecord.getMetadata())));

    assertResults(mds, SearchRequest.of("name*").addNamespace(ns).setLimit(3).build(),
                  appRecord, datasetRecord, programRecord);

    // ascending sort by name. offset and limit should be respected.
    assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setLimit(2)
                    .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC)).build(),
                  appRecord, datasetRecord);
    // return 2 with offset 1 in ascending order
    assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setOffset(1).setLimit(2)
                    .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC)).build(),
                  datasetRecord, programRecord);
    // first 2 in descending order
    assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setLimit(2)
                    .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.DESC)).build(),
                  programRecord, datasetRecord);
    // third one in descending order
    assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setOffset(2).setLimit(1)
                    .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.DESC)).build(),
                  appRecord);
    // test cursors
    SearchResponse response =
      assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setLimit(1).setCursorRequested(true)
                      .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC)).build(),
                    appRecord);
    Assert.assertNotNull(response.getCursor());
    response =
      assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setLimit(1).setCursorRequested(true)
                      .setCursor(response.getCursor()).setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC))
                      .build(),
                    datasetRecord);
    Assert.assertNotNull(response.getCursor());
    response =
      assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setLimit(1).setCursorRequested(true)
                      .setCursor(response.getCursor()).setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC))
                      .build(),
                    programRecord);
    Assert.assertNull(response.getCursor());

    // test cursors with page size 2
    response =
      assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setLimit(2).setCursorRequested(true)
                      .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC)).build(),
                    appRecord, datasetRecord);
    Assert.assertNotNull(response.getCursor());
    Assert.assertEquals(3, response.getTotalResults());
    response =
      assertInOrder(mds, SearchRequest.of("*").addNamespace(ns).setLimit(2).setCursorRequested(true)
                      .setCursor(response.getCursor()).setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC))
                      .build(),
                    programRecord);
    Assert.assertNull(response.getCursor());
    // TODO (CDAP-14584) expect 3 here after this bug is fixed
    // Assert.assertEquals(3, response.getTotalResults());

    // clean up
    mds.batch(batch(new Drop(app), new Drop(dataset), new Drop(program)));
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
    List<MetadataRecord> results = mds.search(request).getResults();
    Assert.assertTrue(results.isEmpty());
  }

  protected static SearchResponse assertInOrder(MetadataStorage mds, SearchRequest request,
                                                MetadataRecord... expectedResults)
    throws IOException {
    SearchResponse response = mds.search(request);
    Assert.assertEquals(ImmutableList.copyOf(expectedResults), response.getResults());
    return response;
  }

  protected static SearchResponse assertResults(MetadataStorage mds, SearchRequest request,
                                                MetadataRecord firstResult, MetadataRecord... expectedResults)
    throws IOException {
    SearchResponse response = mds.search(request);
    Assert.assertEquals(ImmutableSet.<MetadataRecord>builder().add(firstResult).add(expectedResults).build(),
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
}
