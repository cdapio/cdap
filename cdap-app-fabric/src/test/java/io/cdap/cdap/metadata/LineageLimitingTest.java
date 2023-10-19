/*
 * Copyright © 2022 Cask Data, Inc.
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

import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import io.cdap.cdap.api.lineage.field.EndPoint;
import io.cdap.cdap.api.lineage.field.InputField;
import io.cdap.cdap.api.lineage.field.Operation;
import io.cdap.cdap.api.lineage.field.ReadOperation;
import io.cdap.cdap.api.lineage.field.TransformOperation;
import io.cdap.cdap.api.lineage.field.WriteOperation;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.service.RetryStrategyType;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.data2.metadata.lineage.AccessType;
import io.cdap.cdap.data2.metadata.lineage.LineageStoreReader;
import io.cdap.cdap.data2.metadata.lineage.field.EndPointField;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageInfo;
import io.cdap.cdap.data2.metadata.lineage.field.FieldLineageReader;
import io.cdap.cdap.data2.metadata.writer.FieldLineageWriter;
import io.cdap.cdap.data2.metadata.writer.LineageWriter;
import io.cdap.cdap.data2.metadata.writer.MessagingLineageWriter;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.messaging.store.TableFactory;
import io.cdap.cdap.messaging.store.leveldb.LevelDBTableFactory;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.DatasetId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.metadata.lineage.ProgramRunOperations;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for limiting lineage publishing for large lineages using {@link MessagingLineageWriter}.
 */
public class LineageLimitingTest extends AppFabricTestBase {
  private final DatasetId dataset1 = NamespaceId.DEFAULT.dataset("dataset1");
  private final DatasetId dataset2 = NamespaceId.DEFAULT.dataset("dataset2");

  private final ProgramId service1 = NamespaceId.DEFAULT.app("app1").program(ProgramType.SERVICE, "service1");

  private final ProgramId spark1 = NamespaceId.DEFAULT.app("app2").program(ProgramType.SPARK, "spark1");

  @BeforeClass
  public static void beforeClass() throws Throwable {
    CConfiguration cConfiguration = createBasicCConf();
    // use a fast retry strategy with not too many retries, to speed up the test
    String prefix = "system.metadata.";
    cConfiguration.set(prefix + Constants.Retry.TYPE, RetryStrategyType.FIXED_DELAY.toString());
    cConfiguration.set(prefix + Constants.Retry.MAX_RETRIES, "100");
    cConfiguration.set(prefix + Constants.Retry.MAX_TIME_SECS, "10");
    cConfiguration.set(prefix + Constants.Retry.DELAY_BASE_MS, "200");
    cConfiguration.set(Constants.Metadata.MESSAGING_RETRIES_ON_CONFLICT, "20");
    // set a small limit so that no lineages get published in the test
    cConfiguration.set(Constants.Metadata.MESSAGING_PUBLISH_SIZE_LIMIT, "1000");
    // use a messaging service that helps reproduce race conditions in metadata consumption
    initializeAndStartServices(cConfiguration, new PrivateModule() {
      @Override
      protected void configure() {
        bind(TableFactory.class).to(LevelDBTableFactory.class).in(Scopes.SINGLETON);
        bind(MessagingService.class).to(MetadataSubscriberServiceTest.DelayMessagingService.class).in(Scopes.SINGLETON);
        expose(MessagingService.class);
      }
    });
  }

  @Test
  public void testLineageLimiting() throws InterruptedException, ExecutionException, TimeoutException {
    LineageStoreReader lineageReader = getInjector().getInstance(LineageStoreReader.class);
    ProgramRunId run1 = service1.run(RunIds.generate());

    // Write out some lineage information
    LineageWriter lineageWriter = getInjector().getInstance(MessagingLineageWriter.class);
    lineageWriter.addAccess(run1, dataset1, AccessType.READ);
    lineageWriter.addAccess(run1, dataset2, AccessType.WRITE);

    // Write the field level lineage
    FieldLineageWriter fieldLineageWriter = getInjector().getInstance(MessagingLineageWriter.class);
    ProgramRunId spark1Run1 = spark1.run(RunIds.generate(100));
    ReadOperation read = new ReadOperation("read", "some read", EndPoint.of("ns", "endpoint1"), "offset", "body");
    TransformOperation parse = new TransformOperation("parse", "parse body",
                                                      Collections.singletonList(InputField.of("read", "body")),
                                                      "name", "address");
    WriteOperation write = new WriteOperation("write", "write data", EndPoint.of("ns", "endpoint2"),
                                              Arrays.asList(InputField.of("read", "offset"),
                                                            InputField.of("parse", "name"),
                                                            InputField.of("parse", "address")));

    List<Operation> operations = new ArrayList<>();
    operations.add(read);
    operations.add(write);
    operations.add(parse);
    FieldLineageInfo info1 = new FieldLineageInfo(operations);
    fieldLineageWriter.write(spark1Run1, info1);

    ProgramRunId spark1Run2 = spark1.run(RunIds.generate(200));
    fieldLineageWriter.write(spark1Run2, info1);

    // Verifies lineage has been written as it is smaller than maximum specified size
    Set<NamespacedEntityId> expectedLineage = new HashSet<>(Arrays.asList(run1.getParent(), dataset1, dataset2));
    Tasks.waitFor(true, () -> expectedLineage.equals(lineageReader.getEntitiesForRun(run1)),
                  10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    FieldLineageReader fieldLineageReader = getInjector().getInstance(FieldLineageReader.class);

    // Verifies that empty lineage has been written
    EndPointField endPointField = new EndPointField(EndPoint.of("ns", "endpoint2"), "offset");
    List<ProgramRunOperations> incomingOperations =
      fieldLineageReader.getIncomingOperations(endPointField, 1L, Long.MAX_VALUE - 1);
    Assert.assertTrue(incomingOperations.isEmpty());
  }
}
