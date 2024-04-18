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

package io.cdap.cdap.internal.app.services;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Injector;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.SourceControlManagement;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.id.Id.Namespace;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.AppFabricTestHelper;
import io.cdap.cdap.internal.app.store.NamespaceSourceControlMetadataStore;
import io.cdap.cdap.internal.app.store.RepositorySourceControlMetadataStore;
import io.cdap.cdap.proto.SourceControlMetadataRecord;
import io.cdap.cdap.proto.element.EntityType;
import io.cdap.cdap.proto.id.ApplicationReference;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.sourcecontrol.AuthConfig;
import io.cdap.cdap.proto.sourcecontrol.AuthType;
import io.cdap.cdap.proto.sourcecontrol.PatConfig;
import io.cdap.cdap.proto.sourcecontrol.Provider;
import io.cdap.cdap.proto.sourcecontrol.RepositoryConfig;
import io.cdap.cdap.proto.sourcecontrol.SourceControlMeta;
import io.cdap.cdap.sourcecontrol.ApplicationManager;
import io.cdap.cdap.sourcecontrol.AuthenticationConfigException;
import io.cdap.cdap.sourcecontrol.NoChangesToPushException;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPullAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.MultiPushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.NamespaceRepository;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PullAppResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppOperationRequest;
import io.cdap.cdap.sourcecontrol.operationrunner.PushAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryApp;
import io.cdap.cdap.sourcecontrol.operationrunner.RepositoryAppsResponse;
import io.cdap.cdap.sourcecontrol.operationrunner.SourceControlOperationRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.RepositoryTable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;

public class NamespaceSourceControlMetadataRefreshServiceTest {

  private static TransactionRunner transactionRunner;
  private static CConfiguration cConf;
  private static NamespaceAdmin namespaceAdmin;
  private static NamespaceId NAMESPACE_ID = new NamespaceId(Id.Namespace.DEFAULT.getId());
  private static RepositoryConfig REPO_CONFIG = new RepositoryConfig.Builder().setProvider(
          Provider.GITHUB)
      .setLink("testUrl")
      .setAuth(new AuthConfig(AuthType.PAT, new PatConfig("password", "user")))
      .build();
  private static NamespaceSourceControlMetadataRefreshService refreshService;
  private static final String TYPE = EntityType.APPLICATION.toString();

  @BeforeClass
  public static void beforeClass() throws Exception {
    Injector injector = AppFabricTestHelper.getInjector();
    AppFabricTestHelper.ensureNamespaceExists(NAMESPACE_ID.DEFAULT);
    transactionRunner = injector.getInstance(TransactionRunner.class);
    cConf = injector.getInstance(CConfiguration.class);
    namespaceAdmin = injector.getInstance(NamespaceAdmin.class);
    cConf.set(SourceControlManagement.METADATA_REFRESH_INTERVAL_SECONDS, "300");
    cConf.set(SourceControlManagement.METADATA_REFRESH_BUFFER_SECONDS, "120");
    refreshService = new NamespaceSourceControlMetadataRefreshService(
        cConf, transactionRunner, sourceControlOperationRunnerSpy, NAMESPACE_ID,
        namespaceAdmin);
  }

  private static final Instant fixedInstant = Instant.ofEpochSecond(1646358109);
  private static final Instant instantGreaterThanStartRefresh = Instant.ofEpochMilli(
      System.currentTimeMillis() + 100000000);
  private static final SourceControlOperationRunner sourceControlOperationRunnerSpy =
      spy(new MockSourceControlOperationRunner());

  @Test
  public void testRefresh() throws Exception {
    RepositoryAppsResponse expectedListResult = new RepositoryAppsResponse(
        Arrays.asList(
            new RepositoryApp("app1", "hash1"),
            new RepositoryApp("app3", "hash3"),
            new RepositoryApp("app4", "hash4"),
            new RepositoryApp("app5", "hash5"),
            new RepositoryApp("app6", "hash6"),
            new RepositoryApp("app7", "hashDiff")
        )
    );

    setRepository();

    doReturn(expectedListResult)
        .when(sourceControlOperationRunnerSpy).list(Mockito.any(NamespaceRepository.class));

    insertRepoSourceControlMetadataTests();
    insertNamespaceSourceControlTests();

    List<SourceControlMetadataRecord> expectedRecords = new ArrayList<>();
    List<SourceControlMetadataRecord> gotRecords = new ArrayList<>();

    refreshService.runOneIteration();

    // Checking the all apps stored in repo source control table
    gotRecords = getRepoRecords(NAMESPACE_ID.getNamespace());
    expectedRecords = getUpdatedRepoRecords();

    Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

    // Checking the all apps stored in namespace source control table
    gotRecords = getNamespaceRecords(NAMESPACE_ID.getNamespace());
    expectedRecords = getUpdatedNamespaceRecords();

    Assert.assertArrayEquals(expectedRecords.toArray(), gotRecords.toArray());

    deleteAll();

  }

  private List<SourceControlMetadataRecord> getUpdatedRepoRecords() {
    SourceControlMetadataRecord record1 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app1", null, null, fixedInstant.toEpochMilli(), true);
    SourceControlMetadataRecord record2 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app3", null, null, null, false);
    SourceControlMetadataRecord record3 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app4", null, null, instantGreaterThanStartRefresh.toEpochMilli(), true);
    SourceControlMetadataRecord record4 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app5", null, null, instantGreaterThanStartRefresh.toEpochMilli(), false);
    SourceControlMetadataRecord record5 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app6", null, null, fixedInstant.toEpochMilli(), true);
    SourceControlMetadataRecord record6 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app7", null, null, fixedInstant.toEpochMilli(), false);
    List<SourceControlMetadataRecord> insertedRecords = Arrays.asList(record1, record2, record3,
        record4,
        record5, record6);
    return insertedRecords;
  }

  private List<SourceControlMetadataRecord> getUpdatedNamespaceRecords() {
    SourceControlMetadataRecord record1 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app1", "hash1", "commit1",
        fixedInstant.toEpochMilli(), true);
    SourceControlMetadataRecord record2 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app4", "hash4", "commit1", instantGreaterThanStartRefresh.toEpochMilli(), true);
    SourceControlMetadataRecord record3 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app5", "hash5", "commit1", instantGreaterThanStartRefresh.toEpochMilli(), false);
    SourceControlMetadataRecord record4 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app6", "hash6", "commit1",
        fixedInstant.toEpochMilli(), true);
    SourceControlMetadataRecord record5 = new SourceControlMetadataRecord(Namespace.DEFAULT.getId(),
        TYPE, "app7", "hash7", "commit1",
        fixedInstant.toEpochMilli(), false);
    List<SourceControlMetadataRecord> insertedRecords = Arrays.asList(record1, record2, record3,
        record4,
        record5);
    return insertedRecords;
  }

  private void insertRepoSourceControlMetadataTests() {
    insertRepoRecord(Namespace.DEFAULT.getId(), "app1", TYPE, null, null, 0L, false);
    insertRepoRecord(Namespace.DEFAULT.getId(), "app2", TYPE, null, null,
        Instant.now().toEpochMilli(), false);
  }

  private void insertRepoRecord(String namespace, String name, String type,
      String specHash, String commitId, Long lastModified, Boolean isSycned) {
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);
      store.write(new ApplicationReference(namespace, name), isSycned, lastModified);
    });
  }

  private void insertNamespaceSourceControlTests() {
    insertNamespaceRecord(Namespace.DEFAULT.getId(), "app1", TYPE, "hash1", "commit1",
        fixedInstant.toEpochMilli(), false);
    insertNamespaceRecord(Namespace.DEFAULT.getId(), "app4", TYPE, "hash4", "commit1",
        instantGreaterThanStartRefresh.toEpochMilli(), true);
    insertNamespaceRecord(Namespace.DEFAULT.getId(), "app5", TYPE, "hash5", "commit1",
        instantGreaterThanStartRefresh.toEpochMilli(), false);

    insertNamespaceRecord(Namespace.DEFAULT.getId(), "app6", TYPE, "hash6", "commit1",
        fixedInstant.toEpochMilli(), false);
    insertNamespaceRecord(Namespace.DEFAULT.getId(), "app7", TYPE, "hash7", "commit1",
        fixedInstant.toEpochMilli(), false);
  }

  private void insertNamespaceRecord(String namespace, String name, String type,
      String specHash, String commitId, Long lastModified, Boolean isSycned) {
    TransactionRunners.run(transactionRunner, context -> {
      NamespaceSourceControlMetadataStore store = NamespaceSourceControlMetadataStore.create(
          context);
      store.write(new ApplicationReference(namespace, name),
          new SourceControlMeta(specHash, commitId, Instant.ofEpochMilli(lastModified), isSycned));
    });
  }

  private List<SourceControlMetadataRecord> getRepoRecords(String namespace) {
    AtomicReference<List<SourceControlMetadataRecord>> records = new AtomicReference<>(
        new ArrayList<>());
    TransactionRunners.run(transactionRunner, context -> {
      RepositorySourceControlMetadataStore store = RepositorySourceControlMetadataStore.create(
          context);
      records.set(store.getAll(namespace, TYPE));
    });
    return records.get();
  }

  private List<SourceControlMetadataRecord> getNamespaceRecords(String namespace) {
    AtomicReference<List<SourceControlMetadataRecord>> records = new AtomicReference<>(
        new ArrayList<>());
    TransactionRunners.run(transactionRunner, context -> {
      NamespaceSourceControlMetadataStore store = NamespaceSourceControlMetadataStore.create(
          context);
      records.set(store.getAll(namespace, TYPE));
    });
    return records.get();
  }

  private void setRepository() {
    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable store = new RepositoryTable(context);
      store.create(NAMESPACE_ID, REPO_CONFIG);
    });
  }

  private void deleteAll() {
    TransactionRunners.run(transactionRunner, context -> {
      RepositoryTable repoTableStore = new RepositoryTable(context);
      repoTableStore.delete(NAMESPACE_ID);
      NamespaceSourceControlMetadataStore namespaceStore = NamespaceSourceControlMetadataStore.create(
          context);
      namespaceStore.deleteAll(NAMESPACE_ID.getNamespace());
      RepositorySourceControlMetadataStore repoStore = RepositorySourceControlMetadataStore.create(
          context);
      repoStore.deleteAll(NAMESPACE_ID.getNamespace());
    });
  }

  /**
   * A Mock {@link SourceControlOperationRunner} that can be used for tests.
   */
  private static class MockSourceControlOperationRunner extends
      AbstractIdleService implements
      SourceControlOperationRunner {

    @Override
    public PushAppsResponse push(PushAppOperationRequest pushAppOperationRequest)
        throws NoChangesToPushException, AuthenticationConfigException {
      return null;
    }

    @Override
    public PushAppsResponse multiPush(MultiPushAppOperationRequest pushRequest,
        ApplicationManager appManager)
        throws NoChangesToPushException, AuthenticationConfigException {
      return null;
    }

    @Override
    public PullAppResponse<?> pull(PullAppOperationRequest pullRequest)
        throws NotFoundException, AuthenticationConfigException {
      return null;
    }

    @Override
    public void multiPull(MultiPullAppOperationRequest pullRequest,
        Consumer<PullAppResponse<?>> consumer)
        throws NotFoundException, AuthenticationConfigException {
    }

    @Override
    public RepositoryAppsResponse list(NamespaceRepository nameSpaceRepository)
        throws AuthenticationConfigException, NotFoundException {
      return null;
    }

    @Override
    protected void startUp() throws Exception {
      // no-op.
    }

    @Override
    protected void shutDown() throws Exception {
      // no-op.
    }
  }
}
