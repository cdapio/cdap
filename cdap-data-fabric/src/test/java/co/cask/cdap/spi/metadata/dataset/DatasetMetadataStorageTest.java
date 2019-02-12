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

package co.cask.cdap.spi.metadata.dataset;

import co.cask.cdap.api.metadata.MetadataEntity;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.LocalLocationModule;
import co.cask.cdap.common.guice.NamespaceAdminTestModule;
import co.cask.cdap.common.utils.ImmutablePair;
import co.cask.cdap.data.runtime.StorageModule;
import co.cask.cdap.data.runtime.SystemDatasetRuntimeModule;
import co.cask.cdap.proto.EntityScope;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.auth.context.AuthenticationContextModules;
import co.cask.cdap.security.authorization.AuthorizationEnforcementModule;
import co.cask.cdap.security.authorization.AuthorizationTestModule;
import co.cask.cdap.spi.metadata.Metadata;
import co.cask.cdap.spi.metadata.MetadataMutation.Drop;
import co.cask.cdap.spi.metadata.MetadataMutation.Remove;
import co.cask.cdap.spi.metadata.MetadataMutation.Update;
import co.cask.cdap.spi.metadata.MetadataRecord;
import co.cask.cdap.spi.metadata.MetadataStorage;
import co.cask.cdap.spi.metadata.MetadataStorageTest;
import co.cask.cdap.spi.metadata.ScopedNameOfKind;
import co.cask.cdap.spi.metadata.SearchRequest;
import co.cask.cdap.spi.metadata.SearchResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.tephra.TransactionManager;
import org.apache.tephra.runtime.TransactionInMemoryModule;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static co.cask.cdap.api.metadata.MetadataScope.SYSTEM;
import static co.cask.cdap.api.metadata.MetadataScope.USER;
import static co.cask.cdap.spi.metadata.MetadataKind.PROPERTY;
import static co.cask.cdap.spi.metadata.MetadataKind.TAG;

public class DatasetMetadataStorageTest extends MetadataStorageTest {

  private static TransactionManager txManager;
  private static DatasetMetadataStorage storage;

  @BeforeClass
  public static void setup() throws IOException {
    Injector injector = Guice.createInjector(
      new ConfigModule(),
      new LocalLocationModule(),
      new TransactionInMemoryModule(),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new NamespaceAdminTestModule(),
      new AuthorizationTestModule(),
      new AuthorizationEnforcementModule().getInMemoryModules(),
      new AuthenticationContextModules().getMasterModule(),
      new StorageModule()
    );
    txManager = injector.getInstance(TransactionManager.class);
    txManager.startAndWait();
    storage = injector.getInstance(DatasetMetadataStorage.class);
    storage.createIndex();
  }

  @AfterClass
  public static void teardown() throws IOException {
    txManager.stopAndWait();
    storage.dropIndex();
  }

  @Override
  protected MetadataStorage getMetadataStorage() {
    return storage;
  }

  // this tests is not in MetadataStorageTest,
  // because it tests result scoring and sorting specific to the dataset-based implementation
  @Test
  public void testSearchWeight() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    String ns = "ns1";
    NamespaceId nsId = new NamespaceId(ns);
    MetadataEntity service1 = nsId.app("app1").service("service1").toMetadataEntity();
    MetadataEntity dataset1 = nsId.dataset("ds1").toMetadataEntity();
    MetadataEntity dataset2 = nsId.dataset("ds2").toMetadataEntity();

    // Add metadata
    String multiWordValue = "aV1 av2 ,  -  ,  av3 - av4_av5 av6";
    Map<String, String> userProps = ImmutableMap.of("key1", "value1", "key2", "value2", "multiword", multiWordValue);
    Map<String, String> systemProps = ImmutableMap.of("sysKey1", "sysValue1");
    Set<String> userTags = ImmutableSet.of("tag1", "tag2");
    Set<String> temporaryUserTags = ImmutableSet.of("tag3", "tag4");
    Map<String, String> dataset1UserProps = ImmutableMap.of("sKey1", "sValuee1 sValuee2");
    Map<String, String> dataset2UserProps = ImmutableMap.of("sKey1", "sValue1 sValue2", "Key1", "Value1");
    Set<String> sysTags = ImmutableSet.of("sysTag1");

    MetadataRecord service1Record = new MetadataRecord(
      service1, union(new Metadata(USER, userTags, userProps), new Metadata(SYSTEM, sysTags, systemProps)));
    mds.apply(new Update(service1Record.getEntity(), service1Record.getMetadata()));

    // dd and then remove some metadata for dataset2
    mds.apply(new Update(dataset2, new Metadata(USER, temporaryUserTags, userProps)));
    mds.apply(new Remove(dataset2, temporaryUserTags.stream()
      .map(tag -> new ScopedNameOfKind(TAG, USER, tag)).collect(Collectors.toSet())));
    mds.apply(new Remove(dataset2, userProps.keySet().stream()
      .map(tag -> new ScopedNameOfKind(PROPERTY, USER, tag)).collect(Collectors.toSet())));

    MetadataRecord dataset1Record = new MetadataRecord(dataset1, new Metadata(USER, tags(), dataset1UserProps));
    MetadataRecord dataset2Record = new MetadataRecord(dataset2, new Metadata(USER, tags(), dataset2UserProps));

    mds.batch(ImmutableList.of(new Update(dataset1Record.getEntity(), dataset1Record.getMetadata()),
                               new Update(dataset2Record.getEntity(), dataset2Record.getMetadata())));

    // Test score and metadata match
    assertInOrder(mds, SearchRequest.of("value1 multiword:av2").addNamespace(ns).build(),
                  service1Record, dataset2Record);
    assertInOrder(mds, SearchRequest.of("value1 sValue*").addNamespace(ns).setLimit(Integer.MAX_VALUE).build(),
                  dataset2Record, dataset1Record, service1Record);
    assertResults(mds, SearchRequest.of("*").addNamespace(ns).setLimit(Integer.MAX_VALUE).build(),
                  dataset2Record, dataset1Record, service1Record);

    // clean up
    mds.batch(ImmutableList.of(new Drop(service1), new Drop(dataset1), new Drop(dataset2)));
  }

  // this test is specific to teh DatasetMetadataStorage, because of the specific way it tests pagination:
  // it requests offsets that are not a multiple of the page size, which is not supported in all implementations.
  @Test
  public void testCrossNamespacePagination() throws IOException {
    MetadataStorage mds = getMetadataStorage();

    NamespaceId ns1Id = new NamespaceId("ns1");
    NamespaceId ns2Id = new NamespaceId("ns2");

    MetadataEntity ns1app1 = ns1Id.app("a1").toMetadataEntity();
    MetadataEntity ns1app2 = ns1Id.app("a2").toMetadataEntity();
    MetadataEntity ns1app3 = ns1Id.app("a3").toMetadataEntity();
    MetadataEntity ns2app1 = ns2Id.app("a1").toMetadataEntity();
    MetadataEntity ns2app2 = ns2Id.app("a2").toMetadataEntity();

    mds.batch(ImmutableList.of(new Update(ns1app1, new Metadata(USER, tags("v1"))),
                               new Update(ns1app2, new Metadata(USER, tags("v1"))),
                               new Update(ns1app3, new Metadata(USER, tags("v1"))),
                               new Update(ns2app1, new Metadata(USER, tags("v1"))),
                               new Update(ns2app2, new Metadata(USER, tags("v1")))));

    MetadataRecord record11 = new MetadataRecord(ns1app1, new Metadata(USER, tags("v1")));
    MetadataRecord record12 = new MetadataRecord(ns1app2, new Metadata(USER, tags("v1")));
    MetadataRecord record13 = new MetadataRecord(ns1app3, new Metadata(USER, tags("v1")));
    MetadataRecord record21 = new MetadataRecord(ns2app1, new Metadata(USER, tags("v1")));
    MetadataRecord record22 = new MetadataRecord(ns2app2, new Metadata(USER, tags("v1")));

    SearchResponse response =
      assertResults(mds, SearchRequest.of("*").setLimit(Integer.MAX_VALUE).setCursorRequested(true).build(),
                    record11, record12, record13, record21, record22);

    // iterate over results to find the order in which they are returned
    Iterator<MetadataRecord> resultIter = response.getResults().iterator();
    MetadataRecord[] results = {
      resultIter.next(), resultIter.next(), resultIter.next(), resultIter.next(), resultIter.next() };

    // get 4 results (guaranteed to have at least one from each namespace), offset 1
    assertResults(mds, SearchRequest.of("*").setCursorRequested(true).setOffset(1).setLimit(4).build(),
                  results[1], results[2], results[3], results[4]);

    // get the first four
    assertResults(mds, SearchRequest.of("*").setCursorRequested(true).setOffset(0).setLimit(4).build(),
                  results[0], results[1], results[2], results[3]);

    // get middle 3
    assertResults(mds, SearchRequest.of("*").setCursorRequested(true).setOffset(1).setLimit(3).build(),
                  results[1], results[2], results[3], results[3]);

    // clean up
    mds.batch(ImmutableList.of(
      new Drop(ns1app1), new Drop(ns1app2), new Drop(ns1app3), new Drop(ns2app1), new Drop(ns2app2)));
  }

  @Test
  public void testNsScopes() {
    // no namespace
    testNsScopes(null, null, EnumSet.allOf(EntityScope.class), false);
    testNsScopes(Collections.emptySet(), null, EnumSet.allOf(EntityScope.class), false);
    // system only
    testNsScopes(ImmutableSet.of("system"), NamespaceId.SYSTEM, EnumSet.of(EntityScope.SYSTEM), false);
    // user namespace only
    testNsScopes(ImmutableSet.of("myns"), new NamespaceId("myns"), EnumSet.of(EntityScope.USER), false);
    // user and system namespace
    testNsScopes(ImmutableSet.of("myns", "system"), new NamespaceId("myns"), EnumSet.allOf(EntityScope.class), false);
    // multiple user namespaces
    testNsScopes(ImmutableSet.of("myns", "yourns"), null, null, true);
    testNsScopes(ImmutableSet.of("myns", "system", "yourns"), null, null, true);
  }

  private void testNsScopes(Set<String> namespaces,
                            NamespaceId expectedNamespace, Set<EntityScope> expectedScopes,
                            boolean expectUnsupportedOperation) {
    if (expectUnsupportedOperation) {
      try {
        DatasetMetadataStorage.determineNamespaceAndScopes(namespaces);
        Assert.fail("Expected unsupported operation");
      } catch (UnsupportedOperationException e) {
        return; // expected
      }
    }
    ImmutablePair<NamespaceId, Set<EntityScope>> pair = DatasetMetadataStorage.determineNamespaceAndScopes(namespaces);
    Assert.assertEquals("namespace does not match for " + namespaces, expectedNamespace, pair.getFirst());
    Assert.assertEquals("scopes don't match for " + namespaces, expectedScopes, pair.getSecond());
  }

  @Test
  public void testCursorOffsetAndLimits() {
    // search without cursor adds one to the limit to determine if there are more results
    testCursorsOffsetsAndLimits(null, false, 0, 10, null, 0, 0, 11, 10);
    testCursorsOffsetsAndLimits(null, false, 5, 10, null, 5, 5, 11, 10);
    // search with request for a cursor does not need to add one - it can tell by the returned cursor
    testCursorsOffsetsAndLimits(null, true, 0, 10, null, 0, 0, 10, 10);
    testCursorsOffsetsAndLimits(null, true, 5, 10, null, 5, 5, 10, 10);
    // search with cursor supersedes offset and limit
    testCursorsOffsetsAndLimits(new Cursor(10, 5, "x"), false, 20, 50, "x", 0, 10, 5, 5);
    testCursorsOffsetsAndLimits(new Cursor(10, 5, "x"), true, 20, 50, "x", 0, 10, 5, 5);
  }

  private void testCursorsOffsetsAndLimits(Cursor cursor, boolean cursorRequested,
                                           int offsetRequested, int limitRequested,
                                           String expectedCursor,
                                           int expectedOffsetToRequest, int expectedOffsetToRespond,
                                           int expectedLimitToRequest, int expectedLimitToRespond) {
    SearchRequest request = SearchRequest.of("*")
      .setCursor(cursor == null ? null : cursor.toString()).setCursorRequested(cursorRequested)
      .setOffset(offsetRequested).setLimit(limitRequested).build();
    DatasetMetadataStorage.CursorAndOffsetInfo info = DatasetMetadataStorage.determineCursorOffsetAndLimits(request);
    Assert.assertEquals(expectedCursor, info.getCursor());
    Assert.assertEquals(expectedOffsetToRequest, info.getOffsetToRequest());
    Assert.assertEquals(expectedOffsetToRespond, info.getOffsetToRespond());
    Assert.assertEquals(expectedLimitToRequest, info.getLimitToRequest());
    Assert.assertEquals(expectedLimitToRespond, info.getLimitToRespond());
  }
}
