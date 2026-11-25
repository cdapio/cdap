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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.artifact.ArtifactClasses;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.id.Id;
import io.cdap.cdap.common.namespace.NamespacePathLocator;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.NamespacedEntityId;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import io.cdap.cdap.security.impersonation.ImpersonatedOpType;
import io.cdap.cdap.security.impersonation.Impersonator;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.transaction.TransactionException;
import io.cdap.cdap.spi.data.transaction.TransactionRunner;
import io.cdap.cdap.spi.data.transaction.TxRunnable;
import io.cdap.cdap.store.StoreDefinition;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for impersonation exception propagation in {@link ArtifactStore}.
 */
public class ArtifactStoreImpersonationTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static Impersonator createControllableImpersonator(
      AtomicReference<Exception> failureRef) {
    return new Impersonator() {
      @Override
      public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable) throws Exception {
        Exception failure = failureRef.get();
        if (failure != null) {
          throw failure;
        }
        return callable.call();
      }

      @Override
      public <T> T doAs(NamespacedEntityId entityId, Callable<T> callable,
          ImpersonatedOpType impersonatedOpType) throws Exception {
        return doAs(entityId, callable);
      }

      @Override
      public UserGroupInformation getUGI(NamespacedEntityId entityId) {
        throw new UnsupportedOperationException();
      }
    };
  }

  private static TransactionRunner createMockTxRunner(Optional<StructuredRow> artifactDataRow)
      throws Exception {
    StructuredTable table = Mockito.mock(StructuredTable.class);
    Mockito.when(table.read(Mockito.anyCollection())).thenReturn(artifactDataRow);
    StructuredTableContext context = Mockito.mock(StructuredTableContext.class);
    Mockito.when(context.getTable(Mockito.any())).thenReturn(table);

    return new TransactionRunner() {
      @Override
      public void run(TxRunnable runnable) throws TransactionException {
        try {
          runnable.run(context);
        } catch (Exception e) {
          throw new TransactionException("tx error", e);
        }
      }
    };
  }

  @Test
  public void testGetArtifactImpersonationPropagatesRuntimeAndWrapsIoException() throws Exception {
    File rootDir = TEMP_FOLDER.newFolder("getStore");
    File storedJar = new File(rootDir, "art.jar");
    Files.write(storedJar.toPath(), "content".getBytes(StandardCharsets.UTF_8));

    String rowJson = "{\"locationPath\":\"" + storedJar.getAbsolutePath()
        + "\",\"meta\":{\"classes\":{\"apps\":[],\"plugins\":[]},\"usableBy\":[],\"properties\":{}}}";
    StructuredRow row = Mockito.mock(StructuredRow.class);
    Mockito.when(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD))
        .thenReturn(rowJson);

    LocationFactory locationFactory = new LocalLocationFactory(rootDir);
    NamespacePathLocator pathLocator = Mockito.mock(NamespacePathLocator.class);
    Mockito.when(pathLocator.get(Mockito.any(NamespaceId.class)))
        .thenReturn(locationFactory.create("ns"));

    AtomicReference<Exception> failureRef = new AtomicReference<>();
    Impersonator impersonator = createControllableImpersonator(failureRef);
    ArtifactStore store = new ArtifactStore(
        CConfiguration.create(), pathLocator, locationFactory, impersonator,
        createMockTxRunner(Optional.of(row)));

    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "myArt", "1.0.0");
    Assert.assertNotNull(store.getArtifact(artifactId));

    IllegalStateException expectedRuntime = new IllegalStateException("ugi lookup failed");
    failureRef.set(expectedRuntime);
    try {
      store.getArtifact(artifactId);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    IOException ioException = new IOException("keytab io failure");
    failureRef.set(ioException);
    RuntimeException wrapped = Assert.assertThrows(
        RuntimeException.class, () -> store.getArtifact(artifactId));
    Assert.assertSame(ioException, wrapped.getCause());
  }

  @Test
  public void testWriteAndDeletePropagateIoExceptionDirectlyAndRuntimeExceptionUnwrapped()
      throws Exception {
    File rootDir = TEMP_FOLDER.newFolder("writeDeleteStore");
    File inputJar = new File(rootDir, "input.jar");
    Files.write(inputJar.toPath(), "jar-bytes".getBytes(StandardCharsets.UTF_8));

    LocationFactory locationFactory = new LocalLocationFactory(rootDir);
    NamespacePathLocator pathLocator = Mockito.mock(NamespacePathLocator.class);
    Mockito.when(pathLocator.get(Mockito.any(NamespaceId.class)))
        .thenReturn(locationFactory.create("ns"));

    AtomicReference<Exception> failureRef = new AtomicReference<>();
    Impersonator impersonator = createControllableImpersonator(failureRef);

    // 1. Test write(...) when copyFileToDestination throws IOException vs RuntimeException.
    ArtifactStore writeStore = new ArtifactStore(
        CConfiguration.create(), pathLocator, locationFactory, impersonator,
        createMockTxRunner(Optional.empty()));
    Id.Artifact artifactId = Id.Artifact.from(Id.Namespace.DEFAULT, "writeArt", "1.0.0");
    ArtifactMeta meta = new ArtifactMeta(ArtifactClasses.builder().build());
    EntityImpersonator entityImpersonator =
        new EntityImpersonator(artifactId.toEntityId(), impersonator);

    IOException writeIo = new IOException("disk full");
    failureRef.set(writeIo);
    try {
      writeStore.write(artifactId, meta, inputJar, entityImpersonator);
      Assert.fail("Expected IOException");
    } catch (IOException actualIo) {
      Assert.assertSame(writeIo, actualIo);
    }

    IllegalArgumentException writeRuntime = new IllegalArgumentException("bad owner");
    failureRef.set(writeRuntime);
    try {
      writeStore.write(artifactId, meta, inputJar, entityImpersonator);
      Assert.fail("Expected IllegalArgumentException");
    } catch (RuntimeException actualRuntime) {
      Assert.assertSame(writeRuntime, actualRuntime);
    }

    // 2. Test delete(...) -> deleteMeta(...) when impersonator throws IOException vs RuntimeException.
    String rowJson = "{\"locationPath\":\"" + inputJar.getAbsolutePath()
        + "\",\"meta\":{\"classes\":{\"apps\":[],\"plugins\":[]},\"usableBy\":[],\"properties\":{}}}";
    StructuredRow row = Mockito.mock(StructuredRow.class);
    Mockito.when(row.getString(StoreDefinition.ArtifactStore.ARTIFACT_DATA_FIELD))
        .thenReturn(rowJson);

    ArtifactStore deleteStore = new ArtifactStore(
        CConfiguration.create(), pathLocator, locationFactory, impersonator,
        createMockTxRunner(Optional.of(row)));

    IOException deleteIo = new IOException("cannot delete artifact file");
    failureRef.set(deleteIo);
    try {
      deleteStore.delete(artifactId);
      Assert.fail("Expected IOException");
    } catch (IOException actualIo) {
      Assert.assertSame(deleteIo, actualIo);
    }

    IllegalStateException deleteRuntime = new IllegalStateException("impersonation state error");
    failureRef.set(deleteRuntime);
    try {
      deleteStore.delete(artifactId);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actualRuntime) {
      Assert.assertSame(deleteRuntime, actualRuntime);
    }
  }
}
