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

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.api.metrics.MetricsCollectionService;
import io.cdap.cdap.common.AuthorizationException;
import io.cdap.cdap.common.BadRequestException;
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
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.distributed.remote.RemoteRuntimeTable;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.auth.context.AuthenticationContextModules;
import io.cdap.cdap.security.spi.authorization.AuthorizationEnforcer;
import io.cdap.cdap.security.spi.authorization.NoOpAuthorizer;
import io.cdap.cdap.spi.data.StructuredTableAdmin;
import io.cdap.cdap.spi.data.TableAlreadyExistsException;
import io.cdap.cdap.spi.data.table.StructuredTableRegistry;
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

import java.io.IOException;

/**
 * Unit test for {@link DirectRuntimeRequestValidator}.
 */
public class DirectRuntimeRequestValidatorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private CConfiguration cConf;
  private TransactionRunner transactionRunner;

  @Before
  public void setup() throws IOException, TableAlreadyExistsException {
    cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().toString());

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
          bind(AuthorizationEnforcer.class).to(NoOpAuthorizer.class);
          bind(TransactionSystemClient.class).to(ConstantTransactionSystemClient.class);
        }
      }
    );

    // Create runtime store definition
    injector.getInstance(StructuredTableRegistry.class).initialize();
    StoreDefinition.RemoteRuntimeStore.createTables(injector.getInstance(StructuredTableAdmin.class), true);

    transactionRunner = injector.getInstance(TransactionRunner.class);
  }

  @After
  public void cleanup() {
    // This clears the StructuredTableRegistry that is backed by InMemoryTableService, which is a singleton per JVM.
    InMemoryTableService.reset();
  }

  @Test
  public void testValid() throws BadRequestException, AuthorizationException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());

    // Insert the run id
    TransactionRunners.run(transactionRunner, context -> {
      RemoteRuntimeTable.create(context).write(programRunId, new SimpleProgramOptions(programRunId.getParent()));
    });

    // Validation should pass
    RuntimeRequestValidator validator = new DirectRuntimeRequestValidator(cConf, transactionRunner);
    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
  }

  @Test (expected = BadRequestException.class)
  public void testInvalid() throws BadRequestException, AuthorizationException {
    ProgramRunId programRunId = NamespaceId.DEFAULT.app("app").workflow("workflow").run(RunIds.generate());

    // Validation should fail
    RuntimeRequestValidator validator = new DirectRuntimeRequestValidator(cConf, transactionRunner);
    validator.validate(programRunId, new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, "/"));
  }
}
