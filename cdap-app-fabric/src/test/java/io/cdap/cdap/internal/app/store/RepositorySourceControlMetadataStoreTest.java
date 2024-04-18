/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.store;

import static org.junit.Assert.assertEquals;

import io.cdap.cdap.app.store.ScanSourceControlMetadataRequest;
import io.cdap.cdap.app.store.SourceControlMetadataFilter;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.proto.SourceControlMetadataRecord;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.sourcecontrol.SortBy;
import io.cdap.cdap.spi.data.SortOrder;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public abstract class RepositorySourceControlMetadataStoreTest {

  private static final String NAMESPACE = "testNamespace";
  private static final Instant LAST_MODIFIED = Instant.now();
  private static final String NAME = "testName";
  private static final ApplicationReference APP_REF = new ApplicationReference(NAMESPACE, NAME);
  private static final Boolean IS_SYNCED = true;

  private static final String TYPE = EntityType.APPLICATION.toString();

  protected static TransactionRunner transactionRunner;

  @Test
  public void testScanWithFiltering() {
    List<SourceControlMetadataRecord> insertedRecords = insertTests();
    List<SourceControlMetadataRecord> testNamespaceRecords =
        SourceControlMetadataTestUtil.filterAndCollectRecords(insertedRecords, NAMESPACE);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnNameAsc =
        SourceControlMetadataTestUtil.sortByNameAscending(testNamespaceRecords);

    TransactionRunners.run(transactionRunner, context -> {
      List<SourceControlMetadataRecord> gotRecords = new ArrayList<>();
      ScanSourceControlMetadataRequest request;
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);

      // verify name filter for testNamespaceRecordsSortedOnNameAsc
      gotRecords.clear();
      String nameContainsFilter = "te";
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setFilter(new SourceControlMetadataFilter(nameContainsFilter, null)).build();
      List<SourceControlMetadataRecord> expectedRecords;
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc.stream()
          .filter(record -> record.getName().toLowerCase().contains(nameContainsFilter))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify UNSYCNED sync status filter for testNamespaceRecordsSortedOnNameAsc
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setFilter(new SourceControlMetadataFilter(null, false)).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc.stream()
          .filter(record -> !record.getIsSynced()).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify SYCNED sync status and name filter for testNamespaceRecordsSortedOnNameAsc
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setFilter(new SourceControlMetadataFilter(nameContainsFilter, true))
          .build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc.stream().filter(
          record -> record.getIsSynced() && record.getName().toLowerCase()
              .contains(nameContainsFilter)).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      store.deleteAll(NAMESPACE);
    });
  }

  @Test
  public void testScanWithDifferentLimit() {
    List<SourceControlMetadataRecord> insertedRecords = insertTests();
    List<SourceControlMetadataRecord> testNamespaceRecords =
        SourceControlMetadataTestUtil.filterAndCollectRecords(insertedRecords, NAMESPACE);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnNameAsc =
        SourceControlMetadataTestUtil.sortByNameAscending(testNamespaceRecords);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnNameDesc =
        SourceControlMetadataTestUtil.sortByNameDescending(testNamespaceRecords);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnLastModifiedDesc =
        SourceControlMetadataTestUtil.sortByLastModifiedDescending(testNamespaceRecords);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnLastModifiedAsc =
        SourceControlMetadataTestUtil.sortByLastModifiedAscending(testNamespaceRecords);

    TransactionRunners.run(transactionRunner, context -> {
      List<SourceControlMetadataRecord> gotRecords = new ArrayList<>();
      ScanSourceControlMetadataRequest request;
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);

      // verify the scan without filters which picks all apps for testNamespace
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE).build();
      List<SourceControlMetadataRecord> expectedRecords;
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc;
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify the scan without filters which picks all apps for testNamespace
      // when last modified is in desc order
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setSortOrder(SortOrder.DESC).setSortOn(SortBy.LAST_SYNCED_AT).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedDesc;
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify the scan without filters which picks all apps for testNamespace
      // when last modified is in asc order
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setSortOrder(SortOrder.ASC).setSortOn(SortBy.LAST_SYNCED_AT).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedAsc;
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify limit for testNamespaceRecordsSortedOnNameDesc
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE).setLimit(3)
          .setSortOrder(SortOrder.DESC).setSortOn(SortBy.NAME).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameDesc.stream().limit(3)
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());
      store.deleteAll(NAMESPACE);
    });
  }

  @Test
  public void testScanWithPagination() {
    List<SourceControlMetadataRecord> insertedRecords = insertTests();
    List<SourceControlMetadataRecord> testNamespaceRecords =
        SourceControlMetadataTestUtil.filterAndCollectRecords(insertedRecords, NAMESPACE);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnNameAsc =
        SourceControlMetadataTestUtil.sortByNameAscending(testNamespaceRecords);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnNameDesc =
        SourceControlMetadataTestUtil.sortByNameDescending(testNamespaceRecords);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnLastModifiedDesc =
        SourceControlMetadataTestUtil.sortByLastModifiedDescending(testNamespaceRecords);
    List<SourceControlMetadataRecord> testNamespaceRecordsSortedOnLastModifiedAsc =
        SourceControlMetadataTestUtil.sortByLastModifiedAscending(testNamespaceRecords);
    TransactionRunners.run(transactionRunner, context -> {
      List<SourceControlMetadataRecord> gotRecords = new ArrayList<>();
      ScanSourceControlMetadataRequest request;
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);

      // verify name filter with pageToken and limit for testNamespaceRecordsSortedOnNameAsc
      gotRecords.clear();
      String nameContains = "d";
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE).setLimit(1)
          .setScanAfter("datafusionquickstart")
          .setFilter(new SourceControlMetadataFilter(nameContains, null)).build();
      List<SourceControlMetadataRecord> expectedRecords;
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc.stream()
          .filter(record -> record.getName().equals("dependent100")).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify UNSYNCED filter with pageToken and limit for testNamespaceRecordsSortedOnNameAsc
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE).setLimit(1)
          .setScanAfter("datafusionquickstart")
          .setFilter(new SourceControlMetadataFilter(null, false)).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc.stream()
          .filter(record -> record.getName().equals("dependent101")).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify SYNCED and name filter with pageToken and limit for testNamespaceRecordsSortedOnLastModifiedDesc
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE).setLimit(1)
          .setScanAfter("dependent100").setSortOrder(SortOrder.DESC).setSortOn(SortBy.NAME)
          .setFilter(new SourceControlMetadataFilter("t", true)).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedDesc.stream()
          .filter(record -> record.getName().equals("datafusionquickstart"))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify UNSYNCED and name filter with pageToken and limit for testNamespaceRecordsSortedOnLastModifiedDesc
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE).setLimit(1)
          .setScanAfter("dependent100").setSortOn(SortBy.LAST_SYNCED_AT)
          .setSortOrder(SortOrder.DESC)
          .setFilter(new SourceControlMetadataFilter("t", false)).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedDesc.stream()
          .filter(record -> record.getName().equals("dependent101")).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify page token for testNamespaceRecordsSortedOnNameAsc - test 1
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setScanAfter("datafusionquickstart").setLimit(3).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc.stream().filter(
              record -> record.getName().equals("dependent101") || record.getName()
                  .equals("dependent100") || record.getName().equals("newapp"))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify page token for testNamespaceRecordsSortedOnNameAsc - test 2
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setScanAfter("newapp").setLimit(3).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameAsc.stream().filter(
          record -> record.getName().equals("scm-test") || record.getName().equals("zapapp")
              || record.getName().equals("test")).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify page token for testNamespaceRecordsSortedOnLastModifiedDesc - test 1
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setScanAfter("zapapp").setLimit(5).setSortOrder(SortOrder.DESC)
          .setSortOn(SortBy.LAST_SYNCED_AT).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedDesc.stream().filter(
          record -> record.getName().equals("newapp") || record.getName().equals("scm-test")
              || record.getName().equals("datafusionquickstart")).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify page token for testNamespaceRecordsSortedOnLastModifiedDesc - test 2
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setScanAfter("test").setLimit(3).setSortOrder(SortOrder.DESC)
          .setSortOn(SortBy.LAST_SYNCED_AT).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedDesc.stream().filter(
              record -> record.getName().equals("dependent100") || record.getName()
                  .equals("dependent101") || record.getName().equals("newapp"))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify page token for testNamespaceRecordsSortedOnNameDesc
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setSortOrder(SortOrder.DESC).setSortOn(SortBy.NAME).setScanAfter("test")
          .setLimit(3).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnNameDesc.stream().filter(
          record -> record.getName().equals("scm-test") || record.getName().equals("dependent101")
              || record.getName().equals("newapp")).collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify page token for testNamespaceRecordsSortedOnLastModifiedAsc - test 1
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setScanAfter("scm-test").setLimit(4).setSortOrder(SortOrder.ASC)
          .setSortOn(SortBy.LAST_SYNCED_AT).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedAsc.stream().filter(
              record -> record.getName().equals("zapapp") || record.getName().equals("newapp")
                  || record.getName().equals("dependent101") || record.getName().equals("dependent100"))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      // verify page token for testNamespaceRecordsSortedOnLastModifiedAsc - test 2
      gotRecords.clear();
      request = ScanSourceControlMetadataRequest.builder().setNamespace(NAMESPACE)
          .setScanAfter("datafusionquickstart").setLimit(2).setSortOrder(SortOrder.ASC)
          .setSortOn(SortBy.LAST_SYNCED_AT).build();
      store.scan(request, TYPE, gotRecords::add);
      expectedRecords = testNamespaceRecordsSortedOnLastModifiedAsc.stream().filter(
              record -> record.getName().equals("newapp") || record.getName().equals("scm-test"))
          .collect(Collectors.toList());
      Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

      store.deleteAll(NAMESPACE);
    });
  }

  @Test
  public void testWrite() {
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);
      store.write(APP_REF, IS_SYNCED, LAST_MODIFIED.toEpochMilli());
      SourceControlMetadataRecord record = store.get(APP_REF);
      assertEquals(IS_SYNCED, record.getIsSynced());
      assertEquals(LAST_MODIFIED.toEpochMilli(), record.getLastModified().longValue());
      store.deleteAll(NAMESPACE);
    });
  }

  @Test
  public void testDelete() {
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);
      store.write(APP_REF, IS_SYNCED, LAST_MODIFIED.toEpochMilli());
      SourceControlMetadataRecord record = store.get(APP_REF);
      assertEquals(record, new SourceControlMetadataRecord(NAMESPACE, TYPE, NAME, null, null,
          LAST_MODIFIED.toEpochMilli(), IS_SYNCED));
      store.delete(APP_REF);
      record = store.get(APP_REF);
      assertEquals(null, record);
    });
  }

  private List<SourceControlMetadataRecord> insertTests() {
    List<SourceControlMetadataRecord> records = new ArrayList<>();
    records.add(
        insertRecord(NAMESPACE, "datafusionquickstart", TYPE, null, null, Instant.ofEpochSecond(0),
            true));
    records.add(
        insertRecord(NAMESPACE, "scm-test", TYPE, null, null, Instant.ofEpochSecond(1646358109),
            false));
    records.add(
        insertRecord(NAMESPACE, "dependent100", TYPE, null, null, Instant.ofEpochSecond(1646358113),
            true));
    records.add(
        insertRecord(NAMESPACE, "dependent101", TYPE, null, null, Instant.ofEpochSecond(1646358111),
            false));
    records.add(
        insertRecord(NAMESPACE, "newapp", TYPE, null, null, Instant.ofEpochSecond(1646358110),
            false));
    records.add(
        insertRecord(NAMESPACE, "zapapp", TYPE, null, null, Instant.ofEpochSecond(1646358110),
            true));
    records.add(
        insertRecord(NAMESPACE, "test", TYPE, null, null, Instant.ofEpochSecond(1646358114), true));
    records.add(insertRecord(Namespace.DEFAULT.getId(), "test", TYPE, null, null,
        Instant.ofEpochSecond(1646358116), true));
    return records;
  }

  private SourceControlMetadataRecord insertRecord(String namespace, String name, String type,
      String specHash, String commitId, Instant lastModified, Boolean isSynced) {
    SourceControlMetadataRecord record = new SourceControlMetadataRecord(namespace, type, name,
        specHash, commitId, lastModified.toEpochMilli() == 0L ? null : lastModified.toEpochMilli(),
        isSynced);
    ApplicationReference appRef = new ApplicationReference(namespace, name);
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);
      store.write(appRef, isSynced, lastModified.toEpochMilli());
    });
    return record;
  }
}
