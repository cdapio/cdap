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

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.api.metadata.MetadataScope;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.annotation.Nullable;

import static co.cask.cdap.api.metadata.MetadataScope.SYSTEM;
import static co.cask.cdap.api.metadata.MetadataScope.USER;
import static co.cask.cdap.spi.metadata.MetadataConstants.CREATION_TIME_KEY;
import static co.cask.cdap.spi.metadata.MetadataConstants.ENTITY_NAME_KEY;
import static co.cask.cdap.spi.metadata.MetadataConstants.TTL_KEY;
import static co.cask.cdap.spi.metadata.MetadataKind.PROPERTY;
import static co.cask.cdap.spi.metadata.MetadataKind.TAG;

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

    MetadataEntity entity = ofDataset(DEFAULT_NAMESPACE, "entity");

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
                    new Update(app2, app2Record.getMetadata())));

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

    mds.batch(batch(new Drop(app1), new Drop(app2)));
  }

  @Test
  public void testSearchOnTTL() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity ds = ofDataset("ns1", "ds");
    Metadata metaWithTTL = new Metadata(SYSTEM, props(TTL_KEY, "3600"));
    MetadataRecord dsRecord = new MetadataRecord(ds, metaWithTTL);

    mds.apply(new Update(ds, metaWithTTL));
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

    mds.apply(new Drop(ds));
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

    mds.apply(new Update(entity, meta));
    assertResults(mds, SearchRequest.of("myds").build(), record);
    assertResults(mds, SearchRequest.of("schema:*").build(), record);
    assertResults(mds, SearchRequest.of("properties:schema").build(), record);

    for (String expectedTerm : expectedTermsInIndex) {
      assertResults(mds, SearchRequest.of(expectedTerm).build(), record);
      assertResults(mds, SearchRequest.of("schema:" + expectedTerm).build(), record);
    }

    // clean up
    mds.apply(new Drop(entity));
  }

  @Test
  public void testSearchWithInvalidSchema() throws IOException {
    String invalidSchema = "an invalid schema";
    MetadataEntity entity = MetadataEntity.ofDataset("myDs");
    Metadata meta = new Metadata(SYSTEM, props(MetadataConstants.ENTITY_NAME_KEY, "myDs",
                                               MetadataConstants.SCHEMA_KEY, invalidSchema));
    MetadataRecord record = new MetadataRecord(entity, meta);
    MetadataStorage mds = getMetadataStorage();

    mds.apply(new Update(entity, meta));
    assertResults(mds, SearchRequest.of("myds").build(), record);
    assertResults(mds, SearchRequest.of("schema:*").build(), record);
    assertResults(mds, SearchRequest.of("properties:schema").build(), record);
    assertResults(mds, SearchRequest.of("schema:inval*").build(), record);

    // clean up
    mds.apply(new Drop(entity));
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
                               new Update(field, fieldRecord.getMetadata())));
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
    mds.batch(batch(new Drop(dataset), new Drop(hype), new Drop(field)));
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

    MetadataEntity entity = ofWorkflow(ofApp(DEFAULT_NAMESPACE, "appX"), "wtf");
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

  @Test
  public void testSearchOnTypes() throws Exception {
    MetadataStorage mds = getMetadataStorage();

    MetadataEntity myDs = ofDataset(DEFAULT_NAMESPACE, "myDs");
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

    // searching an invalid type should return nothing
    assertEmpty(mds, SearchRequest.of("x*").addType("invalid").build());

    // clean up
    mds.batch(batch(new Drop(myField1), new Drop(myField2)));
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
                    new Update(dataset, datasetRecord.getMetadata())));

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

    String ns = "ns";
    MetadataEntity program = ofWorker(ofApp(ns, "app1"), "wk1");

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

    String ns1 = "ns1";
    MetadataEntity artifact = ofArtifact(ns1, "artifact", "1.0");
    MetadataEntity sysArtifact = ofArtifact(SYSTEM_NAMESPACE, "artifact", "1.0");

    String multiWordKey = "multiword";
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
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

    MetadataEntity ns1app = ofApp("ns1", "a");
    MetadataEntity ns2app = ofApp("ns2", "a");

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
    MetadataEntity ns1App = ofApp("ns1", appName);
    MetadataEntity ns2App = ofApp("ns2", appName);

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
                .collect(Collectors.toList()));

    testSortedSearch(mds, records, ENTITY_NAME_KEY);
    testSortedSearch(mds, records, CREATION_TIME_KEY);

    // clean up
    mds.batch(entities.stream().map(Drop::new).collect(Collectors.toList()));
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
                .collect(Collectors.toList()));

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

    // clean up
    mds.batch(records.stream().map(MetadataRecord::getEntity).map(Drop::new).collect(Collectors.toList()));
  }

  @Nullable
  private String validateCursorAndOffset(MetadataStorage mds,
                                         int offset, int limit, String cursor, boolean requestCursor,
                                         int expectedReults, int expectedOffset, int expectedLimit,
                                         boolean expectMore, boolean expectCursor)
    throws IOException {
    SearchResponse response = mds.search(SearchRequest.of("*")
                                           .setSorting(new Sorting(ENTITY_NAME_KEY, Sorting.Order.ASC))
                                           .setOffset(offset).setLimit(limit)
                                           .setCursor(cursor).setCursorRequested(requestCursor)
                                           .build());
    Assert.assertEquals(expectedReults, response.getResults().size());
    Assert.assertEquals(expectedOffset, response.getOffset());
    Assert.assertEquals(expectedLimit, response.getLimit());
    Assert.assertEquals(expectMore, response.getTotalResults() > response.getOffset() + response.getResults().size());
    Assert.assertEquals(expectCursor, null != response.getCursor());
    return response.getCursor();
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
