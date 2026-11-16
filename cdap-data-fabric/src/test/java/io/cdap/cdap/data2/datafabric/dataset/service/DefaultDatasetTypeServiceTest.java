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

package io.cdap.cdap.data2.datafabric.dataset.service;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.dataset.module.DatasetDefinitionRegistry;
import io.cdap.cdap.api.dataset.module.DatasetModule;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.DefaultNamespacePathLocator;
import io.cdap.cdap.common.namespace.InMemoryNamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.data2.datafabric.dataset.type.DatasetTypeManager;
import io.cdap.cdap.data2.transaction.DelegatingTransactionSystemClientService;
import io.cdap.cdap.data2.transaction.TransactionSystemClientService;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.DatasetModuleId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.table.field.Range;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import io.cdap.http.BodyConsumer;
import io.cdap.http.HttpResponder;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.HttpResponseStatus;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.Callable;
import org.apache.tephra.inmemory.MinimalTxSystemClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Unit tests for {@link DefaultDatasetTypeService} Guava call paths.
 */
public class DefaultDatasetTypeServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  /**
   * Extension module used by {@link DefaultDatasetTypeService} startup tests.
   */
  public static class DummyExtensionModule implements DatasetModule {
    @Override
    public void register(DatasetDefinitionRegistry registry) {
      // no-op
    }
  }

  @Test
  public void testStartStopUploadCopyAndDeployModules() throws Exception {
    File localDir = TEMP_FOLDER.newFolder();
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, localDir.getAbsolutePath());
    cConf.set(Constants.Dataset.Extensions.MODULES, DummyExtensionModule.class.getName());

    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    NamespacePathLocator pathLocator =
        new DefaultNamespacePathLocator(cConf, locationFactory, namespaceAdmin);
    pathLocator.get(NamespaceId.DEFAULT).mkdirs();

    DatasetTypeManager typeManager = Mockito.mock(DatasetTypeManager.class);
    Impersonator impersonator = new DefaultImpersonator(cConf, null);
    TransactionSystemClientService txClientService =
        new DelegatingTransactionSystemClientService(new MinimalTxSystemClient());
    TransactionRunner txRunner = createEmptyScanTxRunner();

    DefaultDatasetTypeService service = new DefaultDatasetTypeService(
        typeManager,
        namespaceAdmin,
        pathLocator,
        cConf,
        impersonator,
        txClientService,
        txRunner,
        Collections.singletonMap("testModule1", new TestModule1()));

    Assert.assertEquals(Service.State.RUNNING, service.startAndWait());
    Assert.assertEquals(Service.State.RUNNING, txClientService.state());
    Mockito.verify(typeManager).addModule(
        NamespaceId.SYSTEM.datasetModule("testModule1"), TestModule1.class.getName(), null, false);
    Mockito.verify(typeManager).addModule(
        NamespaceId.SYSTEM.datasetModule(DummyExtensionModule.class.getName()),
        DummyExtensionModule.class.getName(), null, false);

    // Exercise createModuleConsumer() + Files.copy(uploadedFile, Locations.newOutputSupplier(tmpLocation))
    DatasetModuleId uploadedId = NamespaceId.DEFAULT.datasetModule("uploadedMod");
    BodyConsumer consumer = service.addModule(uploadedId, TestModule1.class.getName(), false);
    HttpResponder responder = Mockito.mock(HttpResponder.class);
    byte[] jarBytes = "jar-payload-bytes".getBytes(StandardCharsets.UTF_8);
    consumer.chunk(Unpooled.wrappedBuffer(jarBytes), responder);
    consumer.finished(responder);

    ArgumentCaptor<Location> archiveCaptor = ArgumentCaptor.forClass(Location.class);
    Mockito.verify(typeManager).addModule(
        Mockito.eq(uploadedId), Mockito.eq(TestModule1.class.getName()),
        archiveCaptor.capture(), Mockito.eq(false));
    Assert.assertTrue(archiveCaptor.getValue().exists());
    Assert.assertEquals(jarBytes.length, archiveCaptor.getValue().length());
    Mockito.verify(responder).sendStatus(HttpResponseStatus.OK);

    Assert.assertEquals(Service.State.TERMINATED, service.stopAndWait());
    Assert.assertEquals(Service.State.TERMINATED, txClientService.state());
  }

  @Test
  public void testAddModulePropagatesIoAndUncheckedExceptionsWithoutDoubleWrapping() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    namespaceAdmin.create(NamespaceMeta.DEFAULT);
    NamespacePathLocator pathLocator = Mockito.mock(NamespacePathLocator.class);
    DatasetTypeManager typeManager = Mockito.mock(DatasetTypeManager.class);
    Impersonator impersonator = Mockito.mock(Impersonator.class);
    TransactionSystemClientService txClientService =
        new DelegatingTransactionSystemClientService(new MinimalTxSystemClient());

    final IOException expectedIo = new IOException("impersonation io error");
    final IllegalStateException expectedRuntime = new IllegalStateException("impersonation runtime");
    final Exception expectedChecked = new Exception("impersonation checked");

    Mockito.when(impersonator.doAs(Mockito.eq(NamespaceId.DEFAULT), Mockito.any(Callable.class)))
        .thenThrow(expectedIo)
        .thenThrow(expectedRuntime)
        .thenAnswer(inv -> {
          throw expectedChecked;
        });

    DefaultDatasetTypeService service = new DefaultDatasetTypeService(
        typeManager, namespaceAdmin, pathLocator, cConf, impersonator,
        txClientService, createEmptyScanTxRunner(), Collections.emptyMap());

    DatasetModuleId moduleId = NamespaceId.DEFAULT.datasetModule("m1");

    try {
      service.addModule(moduleId, TestModule1.class.getName(), false);
      Assert.fail("Expected IOException");
    } catch (IOException actual) {
      Assert.assertSame(expectedIo, actual);
    }

    try {
      service.addModule(moduleId, TestModule1.class.getName(), false);
      Assert.fail("Expected exact IllegalStateException without double-wrapping");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    try {
      service.addModule(moduleId, TestModule1.class.getName(), false);
      Assert.fail("Expected RuntimeException wrapping checked exception");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedChecked, actual.getCause());
    }
  }

  @Test
  public void testDeployDefaultModulesPropagatesUncheckedFailureOnStartUp() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());

    NamespaceAdmin namespaceAdmin = new InMemoryNamespaceAdmin();
    NamespacePathLocator pathLocator = Mockito.mock(NamespacePathLocator.class);
    DatasetTypeManager typeManager = Mockito.mock(DatasetTypeManager.class);
    final IllegalStateException expectedFailure = new IllegalStateException("deploy default boom");
    Mockito.doThrow(expectedFailure).when(typeManager)
        .addModule(Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.anyBoolean());

    DefaultDatasetTypeService service = new DefaultDatasetTypeService(
        typeManager, namespaceAdmin, pathLocator, cConf, new DefaultImpersonator(cConf, null),
        new DelegatingTransactionSystemClientService(new MinimalTxSystemClient()),
        createEmptyScanTxRunner(),
        Collections.singletonMap("badDefault", new TestModule1()));

    try {
      service.startAndWait();
      Assert.fail("Expected startup failure");
    } catch (RuntimeException e) {
      Assert.assertSame(expectedFailure, e.getCause());
    }
  }

  private static TransactionRunner createEmptyScanTxRunner() throws Exception {
    StructuredTable emptyTable = Mockito.mock(StructuredTable.class);
    Mockito.when(emptyTable.scan(Mockito.any(Range.class), Mockito.anyInt()))
        .thenAnswer(inv -> CloseableIterator.empty());
    Mockito.when(emptyTable.read(Mockito.anyCollection()))
        .thenReturn(java.util.Optional.empty());

    StructuredTableContext tableContext = Mockito.mock(StructuredTableContext.class);
    Mockito.when(tableContext.getTable(Mockito.any())).thenReturn(emptyTable);

    TransactionRunner txRunner = Mockito.mock(TransactionRunner.class);
    Mockito.doAnswer(inv -> {
      inv.getArgument(0, TxRunnable.class).run(tableContext);
      return null;
    }).when(txRunner).run(Mockito.any(TxRunnable.class));
    return txRunner;
  }
}
