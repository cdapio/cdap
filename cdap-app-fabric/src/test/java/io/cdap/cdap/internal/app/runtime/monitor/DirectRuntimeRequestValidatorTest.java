/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.artifact.ArtifactId;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.api.artifact.ArtifactVersion;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.app.store.Store;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.NamespaceAdminTestModule;
import io.cdap.cdap.common.metrics.NoOpMetricsCollectionService;
import io.cdap.cdap.data.runtime.ConstantTransactionSystemClient;
import io.cdap.cdap.data.runtime.DataSetsModules;
import io.cdap.cdap.data.runtime.StorageModule;
import io.cdap.cdap.data.runtime.SystemDatasetRuntimeModule;
import io.cdap.cdap.data2.dataset2.lib.table.inmemory.InMemoryTableService;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.internal.app.store.AppMetadataStore;
import io.cdap.cdap.internal.app.store.DefaultStore;
import io.cdap.cdap.internal.app.store.RunRecordDetail;
import io.cdap.cdap.logging.gateway.handlers.ProgramRunRecordFetcher;
import io.cdap.cdap.messaging.data.MessageId;
import io.cdap.cdap.proto.ProgramRunStatus;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.proto.security.Principal;
import io.cdap.cdap.proto.security.StandardPermission;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.spi.authentication.AuthenticationContext;
import io.cdap.cdap.security.spi.authorization.AccessEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAccessController;
import io.cdap.cdap.security.spi.authorization.UnauthorizedException;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TransactionRunners;
import io.cdap.cdap.store.StoreDefinition;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.tephra.TransactionSystemClient;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;
import javax.annotation.Nullable;

/**
 * Unit test for {@link DirectRuntimeRequestValidator}.
 */
@RunWith(MockitoJUnitRunner.class)
public class DirectRuntimeRequestValidatorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static final ArtifactId ARTIFACT_ID = new ArtifactId("test", new ArtifactVersion("1.0"), ArtifactScope.USER);

  private CConfiguration cConf;
  private TransactionRunner txRunner;

  @Mock
  private AccessEnforcer accessEnforcer;
  @Mock
  private AuthenticationContext authenticationContext;

  @Before
  public void setup() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().toString());
    // This will effectively turn off the cache in the validator with a TTL of 0.
    cConf.setLong(Constants.RuntimeMonitor.POLL_TIME_MS, 0L);

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf),
      new SystemDatasetRuntimeModule().getInMemoryModules(),
      new DataSetsModules().getInMemoryModules(),
      new NamespaceAdminTestModule(),
      new StorageModule(),
      new AuthenticationContextModules().getNoOpModule(),
      new AbstractModule() {
        @Override
        protected void configure() {
          bind(MetricsCollectionService.class).to(NoOpMetricsCollectionService.class);
          bind(AccessEnforcer.class).to(NoOpAccessController.class);
          bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
          bind(Store.class).to(DefaultStore.class);
        }
      }
    );

    // Create store definition
    StoreDefinition.AppMetadataStore.create(injector.getInstance(StructuredTableAdmin.class));

    txRunner = injector.getInstance(TransactionRunner.class);
  }

  @After
  public void cleanup() {
    // This clears the StructuredTableRegistry that is backed by InMemoryTableService, which is a singleton per JVM.
    InMemoryTableService.reset();
  }

  @Test
  public void testValid() throws BadRequestException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());

    // Insert the run
    TransactionRunners.run(txRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.recordProgramProvisioning(programRunId, Collections.emptyMap(),
                                      Collections.singletonMap(SystemArguments.PROFILE_NAME, "system:default"),
                                      createSourceId(1), ARTIFACT_ID);
      store.recordProgramProvisioned(programRunId, 1, createSourceId(2));
      store.recordProgramStart(programRunId, null, Collections.emptyMap(), createSourceId(3));
    });

    // Validation should pass
    RuntimeRequestValidator validator = new DirectRuntimeRequestValidator(cConf, txRunner,
                                                                          new MockProgramRunRecordFetcher(),
                                                                          accessEnforcer, authenticationContext);
    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
  }

  @Test (expected = BadRequestException.class)
  public void testInvalid() throws BadRequestException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());

    // Validation should fail
    RuntimeRequestValidator validator = new DirectRuntimeRequestValidator(cConf, txRunner,
                                                                          new MockProgramRunRecordFetcher(),
                                                                          accessEnforcer, authenticationContext);
    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
  }

  @Test (expected = UnauthorizedException.class)
  public void testUnauthorized() throws BadRequestException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());

    RuntimeRequestValidator validator = new DirectRuntimeRequestValidator(cConf, txRunner,
                                                                          new MockProgramRunRecordFetcher(),
                                                                          accessEnforcer, authenticationContext);

    Principal principal = new Principal("test", Principal.PrincipalType.USER);
    Mockito.when(authenticationContext.getPrincipal()).thenReturn(principal);
    Mockito.doThrow(new UnauthorizedException("Unauthorized"))
      .when(accessEnforcer).enforce(programRunId, principal, StandardPermission.GET);

    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
  }

  @Test (expected = BadRequestException.class)
  public void testNotRunning() throws BadRequestException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());

    // Insert a completed run
    TransactionRunners.run(txRunner, context -> {
      AppMetadataStore store = AppMetadataStore.create(context);
      store.recordProgramProvisioning(programRunId, Collections.emptyMap(),
                                      Collections.singletonMap(SystemArguments.PROFILE_NAME, "system:default"),
                                      createSourceId(1), ARTIFACT_ID);
      store.recordProgramProvisioned(programRunId, 1, createSourceId(2));
      store.recordProgramStart(programRunId, null, Collections.emptyMap(), createSourceId(3));
      store.recordProgramStop(programRunId, System.currentTimeMillis(), ProgramRunStatus.COMPLETED, null,
                              createSourceId(4));
    });

    // Validation should fail
    RuntimeRequestValidator validator = new DirectRuntimeRequestValidator(cConf, txRunner,
                                                                          new MockProgramRunRecordFetcher(),
                                                                          accessEnforcer, authenticationContext);
    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
  }

  @Test
  public void testFetcher() throws BadRequestException {
    ArtifactId artifactId = new ArtifactId("test", new ArtifactVersion("1.0"), ArtifactScope.USER);
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").spark("spark").run(RunIds.generate());
    RunRecordDetail runRecord = RunRecordDetail.builder()
      .setProgramRunId(programRunId)
      .setStartTime(System.currentTimeMillis())
      .setArtifactId(artifactId)
      .setStatus(ProgramRunStatus.RUNNING)
      .setSystemArgs(ImmutableMap.of(
        SystemArguments.PROFILE_NAME, "default",
        SystemArguments.PROFILE_PROVISIONER, "native"))
      .setProfileId(NamespaceId.DEFAULT.profile("native"))
      .setSourceId(new byte[MessageId.RAW_ID_SIZE])
      .build();

    MockProgramRunRecordFetcher runRecordFetcher = new MockProgramRunRecordFetcher().setRunRecord(runRecord);
    RuntimeRequestValidator validator = new DirectRuntimeRequestValidator(cConf, txRunner, runRecordFetcher,
                                                                          accessEnforcer, authenticationContext);

    // The first call should be hitting the run record fetching to fetch the run record.
    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));

    // The second call will hit the runtime store, so it shouldn't matter what the run record fetch returns
    runRecordFetcher.setRunRecord(null);
    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
  }

  private byte[] createSourceId(int value) {
    return Bytes.toBytes(value);
  }

  /**
   * A mock {@link ProgramRunRecordFetcher} that returns a fixed {@link RunRecordDetail} if the run id matches.
   */
  private static final class MockProgramRunRecordFetcher implements ProgramRunRecordFetcher {

    private RunRecordDetail runRecord;

    MockProgramRunRecordFetcher setRunRecord(@Nullable RunRecordDetail runRecord) {
      this.runRecord = runRecord;
      return this;
    }

    @Override
    public RunRecordDetail getRunRecordMeta(ProgramRunId runId) throws NotFoundException {
      if (runRecord == null) {
        throw new NotFoundException(runId);
      }
      if (!runId.equals(runRecord.getProgramRunId())) {
        throw new NotFoundException(runId);
      }
      return runRecord;
    }
  }
}
