/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.app.runtime.spark.distributed;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.runtime.spark.SparkCredentialsUpdater;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContext;
import io.cdap.cdap.app.runtime.spark.SparkRuntimeContextTest;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.utils.Tasks;
import io.cdap.cdap.proto.id.ProgramRunId;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link SparkDriverService}.
 */
public class SparkDriverServiceTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testStartStopCredentialsUpdaterAndHeartbeatStopCommand() throws Exception {
    LocationFactory locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(
        CConfiguration.create(), new Configuration());
    ProgramRunId programRunId = runtimeContext.getProgramRunId();

    SparkExecutionService executionService = new SparkExecutionService(
        locationFactory, InetAddress.getLoopbackAddress().getCanonicalHostName(), programRunId, null);
    executionService.setShutdownWaitSeconds(5L);
    executionService.startAsync().awaitRunning();

    try {
      final SparkDriverService driverService = new SparkDriverService(executionService.getBaseURI(), runtimeContext);

      Location credentialsDir = locationFactory.create("creds");
      credentialsDir.mkdirs();
      SparkCredentialsUpdater updater = new SparkCredentialsUpdater(
          new Supplier<Credentials>() {
            @Override
            public Credentials get() {
              return new Credentials();
            }
          },
          credentialsDir, "creds-file", 60000L, 60000L, 2);
      Field updaterField = SparkDriverService.class.getDeclaredField("credentialsUpdater");
      updaterField.setAccessible(true);
      updaterField.set(driverService, updater);

      driverService.startAsync().awaitRunning();
      Assert.assertTrue(driverService.isRunning());
      Assert.assertTrue(updater.isRunning());

      // Initiating stop on SparkExecutionService causes the next heartbeat to return a STOP SparkCommand,
      // which triggers SparkDriverService.stop() and sets runtimeContext.getTerminationTime().
      executionService.stopAsync();
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() {
          try {
            return runtimeContext.getTerminationTime() > 0L;
          } catch (IllegalStateException e) {
            return false;
          }
        }
      }, 5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

      // Mark program completed so the driver loop exits immediately and shuts down credentialsUpdater.
      driverService.completed();
      Tasks.waitFor(Service.State.TERMINATED, new Callable<Service.State>() {
        @Override
        public Service.State call() {
          return driverService.state();
        }
      }, 5, TimeUnit.SECONDS, 20, TimeUnit.MILLISECONDS);

      Assert.assertEquals(Service.State.TERMINATED, updater.state());
    } finally {
      executionService.shutdownNow();
    }
  }

  @Test
  public void testCredentialsSupplierExceptionPropagation() throws Exception {
    SparkRuntimeContext runtimeContext = SparkRuntimeContextTest.createRuntimeContext(
        CConfiguration.create(), new Configuration());
    SparkDriverService driverService = new SparkDriverService(URI.create("http://127.0.0.1:1"), runtimeContext);

    Method supplierMethod = SparkDriverService.class.getDeclaredMethod(
        "createCredentialsSupplier", SparkExecutionClient.class, Location.class);
    supplierMethod.setAccessible(true);

    Location credentialsDir = new LocalLocationFactory(TEMP_FOLDER.newFolder()).create("supplier-creds");
    credentialsDir.mkdirs();

    // 1. Unchecked RuntimeException must be propagated directly without double-wrapping.
    SparkExecutionClient uncheckedClient = Mockito.mock(SparkExecutionClient.class);
    IllegalStateException expectedUnchecked = new IllegalStateException("unchecked write failure");
    Mockito.doThrow(expectedUnchecked).when(uncheckedClient).writeCredentials(Mockito.any(Location.class));

    @SuppressWarnings("unchecked")
    Supplier<Credentials> uncheckedSupplier =
        (Supplier<Credentials>) supplierMethod.invoke(driverService, uncheckedClient, credentialsDir);
    try {
      uncheckedSupplier.get();
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedUnchecked, caught);
    }

    // 2. Checked IOException must be wrapped in RuntimeException.
    SparkExecutionClient checkedClient = Mockito.mock(SparkExecutionClient.class);
    IOException expectedIo = new IOException("checked write failure");
    Mockito.doThrow(expectedIo).when(checkedClient).writeCredentials(Mockito.any(Location.class));

    @SuppressWarnings("unchecked")
    Supplier<Credentials> checkedSupplier =
        (Supplier<Credentials>) supplierMethod.invoke(driverService, checkedClient, credentialsDir);
    try {
      checkedSupplier.get();
      Assert.fail("Expected RuntimeException wrapping IOException");
    } catch (RuntimeException caught) {
      Assert.assertSame(expectedIo, caught.getCause());
    }
  }
}
