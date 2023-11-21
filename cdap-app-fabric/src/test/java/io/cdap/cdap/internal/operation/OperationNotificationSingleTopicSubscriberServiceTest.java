/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.operation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;
import com.google.gson.Gson;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants.Operation;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.LocalLocationModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.common.utils.ImmutablePair;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.proto.Notification;
import io.cdap.cdap.proto.id.OperationRunId;
import io.cdap.cdap.proto.operation.OperationError;
import io.cdap.cdap.proto.operation.OperationResource;
import io.cdap.cdap.proto.operation.OperationRun;
import io.cdap.cdap.proto.operation.OperationRunStatus;
import io.cdap.cdap.proto.operation.OperationType;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.sql.PostgresInstantiator;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

public class OperationNotificationSingleTopicSubscriberServiceTest extends OperationTestBase {

  private static CConfiguration cConf;
  private static TransactionRunner transactionRunner;
  private static EmbeddedPostgres pg;

  private static final Gson GSON = new Gson();
  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @BeforeClass
  public static void beforeClass() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Operation.STATUS_EVENT_FETCH_SIZE, "3");
    cConf.set(Operation.STATUS_EVENT_POLL_DELAY_MILLIS, "1");
    cConf.set(Operation.STATUS_EVENT_TX_SIZE, "1");
    // addRetryStrategy(cConf, "system.notification.");
    pg = PostgresInstantiator.createAndStart(cConf, TEMP_FOLDER.newFolder());
    Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new LocalLocationModule(),
        new SystemDatasetRuntimeModule().getInMemoryModules(),
        new StorageModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class)
                .in(Scopes.SINGLETON);
          }
        }
    );

    transactionRunner = injector.getInstance(TransactionRunner.class);
    StoreDefinition.OperationRunsStore.create(injector.getInstance(StructuredTableAdmin.class));
  }

  @AfterClass
  public static void afterClass() {
    Closeables.closeQuietly(pg);
  }

  @Test
  public void testProcessMessages() throws Exception {
    MessagingService mockMsgService = Mockito.mock(MessagingService.class);
    OperationStatePublisher mockStatePublisher = Mockito.mock(OperationStatePublisher.class);
    InMemoryOperationRuntime mockRuntime = Mockito.mock(InMemoryOperationRuntime.class);
    OperationLifecycleManager lifecycleManager =
        new OperationLifecycleManager(transactionRunner, mockRuntime, mockStatePublisher);
    OperationNotificationSingleTopicSubscriberService subscriberService =
        new OperationNotificationSingleTopicSubscriberService(
            mockMsgService,
            cConf,
            Mockito.mock(MetricsCollectionService.class),
            mockStatePublisher,
            transactionRunner,
            "name",
            "topic",
            lifecycleManager
        );

    List<OperationRunDetail> details = insertTestRuns(transactionRunner).stream()
        .filter(d -> d.getRun().getStatus().equals(OperationRunStatus.STARTING)).collect(
            Collectors.toList());

    Notification notification1 = new Notification(Notification.Type.OPERATION_STATUS,
        ImmutableMap.<String, String>builder()
            .put(Operation.RUN_ID_NOTIFICATION_KEY, details.get(0).getRunId().toString())
            .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.STARTING.name()).build());

    Notification notification2 = new Notification(Notification.Type.OPERATION_STATUS,
        ImmutableMap.<String, String>builder()
            .put(Operation.RUN_ID_NOTIFICATION_KEY, details.get(1).getRunId().toString())
            .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.STARTING.name()).build());

    TransactionRunners.run(transactionRunner, (context) -> {
      subscriberService.processMessages(
          context,
          ImmutableList.of(ImmutablePair.of("1", notification1),
              ImmutablePair.of("2", notification2)).iterator()
      );
    }, Exception.class);

    Mockito.verify(mockRuntime, Mockito.times(2)).run(Mockito.any());
  }

  @Test
  public void testProcessNotificationInvalidOperation() {
    MessagingService mockMsgService = Mockito.mock(MessagingService.class);
    OperationStatePublisher mockStatePublisher = Mockito.mock(OperationStatePublisher.class);
    InMemoryOperationRuntime mockRuntime = Mockito.mock(InMemoryOperationRuntime.class);
    OperationLifecycleManager lifecycleManager = new OperationLifecycleManager(transactionRunner,
        mockRuntime, mockStatePublisher);
    OperationNotificationSingleTopicSubscriberService subscriberService =
        new OperationNotificationSingleTopicSubscriberService(
            mockMsgService,
            cConf,
            Mockito.mock(MetricsCollectionService.class),
            mockStatePublisher,
            transactionRunner,
            "name",
            "topic",
            lifecycleManager
        );

    Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
        ImmutableMap.<String, String>builder().put(Operation.RUN_ID_NOTIFICATION_KEY,
            GSON.toJson(new OperationRunId(testNamespace, "1"))).build());

    TransactionRunners.run(transactionRunner, context -> {
      subscriberService.processNotification(null, notification, context);
    });
    // no exception should be thrown
  }

  @Test
  public void testProcessNotificationInvalidTransition() throws Exception {
    MessagingService mockMsgService = Mockito.mock(MessagingService.class);
    OperationStatePublisher mockStatePublisher = Mockito.mock(OperationStatePublisher.class);
    InMemoryOperationRuntime mockRuntime = Mockito.mock(InMemoryOperationRuntime.class);
    OperationLifecycleManager lifecycleManager =
        new OperationLifecycleManager(transactionRunner, mockRuntime, mockStatePublisher);
    OperationNotificationSingleTopicSubscriberService subscriberService =
        new OperationNotificationSingleTopicSubscriberService(
            mockMsgService,
            cConf,
            Mockito.mock(MetricsCollectionService.class),
            mockStatePublisher,
            transactionRunner,
            "name",
            "topic",
            lifecycleManager
        );

    OperationRunDetail detail = insertRun(testNamespace, OperationType.PULL_APPS,
        OperationRunStatus.RUNNING, transactionRunner);

    Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
        ImmutableMap.<String, String>builder()
            .put(Operation.RUN_ID_NOTIFICATION_KEY, GSON.toJson(detail.getRunId()))
            .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.STARTING.name()).build());

    TransactionRunners.run(transactionRunner, context -> {
      subscriberService.processNotification("1".getBytes(), notification, context);
    });
  }

  @Test
  public void testProcessNotification() throws Exception {
    MessagingService mockMsgService = Mockito.mock(MessagingService.class);
    OperationStatePublisher mockStatePublisher = Mockito.mock(OperationStatePublisher.class);
    InMemoryOperationRuntime mockRuntime = Mockito.mock(InMemoryOperationRuntime.class);
    OperationLifecycleManager lifecycleManager = new OperationLifecycleManager(transactionRunner,
        mockRuntime, mockStatePublisher);
    OperationNotificationSingleTopicSubscriberService subscriberService =
        new OperationNotificationSingleTopicSubscriberService(
            mockMsgService,
            cConf,
            Mockito.mock(MetricsCollectionService.class),
            mockStatePublisher,
            transactionRunner,
            "name",
            "topic",
            lifecycleManager
        );

    // process PENDING
    OperationRunDetail detail = insertRun(testNamespace, OperationType.PULL_APPS,
        OperationRunStatus.STARTING, transactionRunner);

    // process STARTING
    TransactionRunners.run(transactionRunner, context -> {
      Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
          ImmutableMap.<String, String>builder()
              .put(Operation.RUN_ID_NOTIFICATION_KEY, detail.getRunId().toString())
              .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.STARTING.name())
              .build());
      subscriberService.processNotification("1".getBytes(), notification, context);
    });

    Mockito.verify(mockRuntime, Mockito.times(1)).run(detail);
    Assert.assertEquals(getRun(detail.getRunId(), transactionRunner).getRun().getStatus(),
        OperationRunStatus.STARTING);

    // process RUNNING
    Set<OperationResource> resources = ImmutableSet.of(new OperationResource("1"));
    TransactionRunners.run(transactionRunner, context -> {
      Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
          ImmutableMap.<String, String>builder()
              .put(Operation.RUN_ID_NOTIFICATION_KEY, detail.getRunId().toString())
              .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.RUNNING.name())
              .put(Operation.RESOURCES_NOTIFICATION_KEY, GSON.toJson(resources))
              .build());
      subscriberService.processNotification("1".getBytes(), notification, context);
    });

    OperationRun run = getRun(detail.getRunId(), transactionRunner).getRun();
    Assert.assertEquals(run.getStatus(), OperationRunStatus.RUNNING);
    Assert.assertEquals(run.getMetadata().getResources(), resources);

    // process SUCCEEDED
    Set<OperationResource> resources2 = ImmutableSet.of(new OperationResource("2"));
    TransactionRunners.run(transactionRunner, context -> {
      Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
          ImmutableMap.<String, String>builder()
              .put(Operation.RUN_ID_NOTIFICATION_KEY, detail.getRunId().toString())
              .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.RUNNING.name())
              .put(Operation.RESOURCES_NOTIFICATION_KEY, GSON.toJson(resources2)).build());
      subscriberService.processNotification("1".getBytes(), notification, context);
    });

    run = getRun(detail.getRunId(), transactionRunner).getRun();
    Assert.assertEquals(run.getStatus(), OperationRunStatus.RUNNING);
    Assert.assertEquals(run.getMetadata().getResources(), resources2);

    // process FAILED
    OperationRunDetail detail2 = insertRun(testNamespace, OperationType.PULL_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    OperationError error = new OperationError("test", null);
    TransactionRunners.run(transactionRunner, context -> {
      Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
          ImmutableMap.<String, String>builder()
              .put(Operation.RUN_ID_NOTIFICATION_KEY, detail2.getRunId().toString())
              .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.FAILED.name())
              .put(Operation.ERROR_NOTIFICATION_KEY, GSON.toJson(error)).build());
      subscriberService.processNotification("1".getBytes(), notification, context);
    });

    run = getRun(detail2.getRunId(), transactionRunner).getRun();
    Assert.assertEquals(run.getStatus(), OperationRunStatus.FAILED);
    Assert.assertEquals(run.getError(), error);

    // process STOPPING
    OperationRunDetail detail3 = insertRun(testNamespace, OperationType.PULL_APPS,
        OperationRunStatus.RUNNING, transactionRunner);
    OperationController controller = Mockito.mock(OperationController.class);
    Mockito.when(mockRuntime.getController(detail3)).thenReturn(controller);
    TransactionRunners.run(transactionRunner, context -> {
      Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
          ImmutableMap.<String, String>builder()
              .put(Operation.RUN_ID_NOTIFICATION_KEY, detail3.getRunId().toString())
              .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.STOPPING.name())
              .build());
      subscriberService.processNotification("1".getBytes(), notification, context);
    });

    run = getRun(detail3.getRunId(), transactionRunner).getRun();
    Assert.assertEquals(run.getStatus(), OperationRunStatus.STOPPING);
    Mockito.verify(controller, Mockito.times(1)).stop();

    // process KILLED
    TransactionRunners.run(transactionRunner, context -> {
      Notification notification = new Notification(Notification.Type.OPERATION_STATUS,
          ImmutableMap.<String, String>builder()
              .put(Operation.RUN_ID_NOTIFICATION_KEY, detail3.getRunId().toString())
              .put(Operation.STATUS_NOTIFICATION_KEY, OperationRunStatus.KILLED.name())
              .build());
      subscriberService.processNotification("1".getBytes(), notification, context);
    });

    run = getRun(detail3.getRunId(), transactionRunner).getRun();
    Assert.assertEquals(run.getStatus(), OperationRunStatus.KILLED);
  }
}

