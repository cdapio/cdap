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

package io.cdap.cdap.internal.app.runtime.monitor;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.app.guice.ClusterMode;
import io.cdap.cdap.app.runtime.ProgramOptions;
import io.cdap.cdap.common.app.RunIds;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.internal.remote.GceRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.NoOpRemoteAuthenticator;
import io.cdap.cdap.common.internal.remote.RemoteClient;
import io.cdap.cdap.internal.app.runtime.BasicArguments;
import io.cdap.cdap.internal.app.runtime.SimpleProgramOptions;
import io.cdap.cdap.internal.app.runtime.SystemArguments;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.proto.id.ProgramId;
import io.cdap.cdap.proto.id.ProgramRunId;
import io.cdap.cdap.security.spi.authenticator.RemoteAuthenticator;
import java.util.Arrays;
import java.util.Collections;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for {@link RuntimeMonitors}.
 */
public class RuntimeMonitorsTest {

  private String originalRoutingBaseUri;

  @Before
  public void setUp() {
    originalRoutingBaseUri = System.getProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI);
    System.clearProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI);
  }

  @After
  public void tearDown() {
    if (originalRoutingBaseUri == null) {
      System.clearProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI);
    } else {
      System.setProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI, originalRoutingBaseUri);
    }
  }

  @Test
  public void testSetupMonitoringRejectsSsh() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "https://monitor.example.com");
    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());
    Injector injector = createInjector(cConf, runId);
    ProgramOptions opts = new SimpleProgramOptions(
        runId.getParent(),
        new BasicArguments(Collections.emptyMap()),
        new BasicArguments(Collections.singletonMap(SystemArguments.RUNTIME_MONITOR_TYPE, "ssh")));

    UnsupportedOperationException e = Assert.assertThrows(UnsupportedOperationException.class,
        () -> RuntimeMonitors.setupMonitoring(injector, opts));
    Assert.assertTrue(e.getMessage().contains("Only URL runtime monitoring is supported"));
    Assert.assertNull(System.getProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI));
  }

  @Test
  public void testSetupMonitoringRejectsMissingEmptyAndWhitespaceMonitorUrl() {
    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());
    ProgramOptions opts = new SimpleProgramOptions(
        runId.getParent(), new BasicArguments(), new BasicArguments());

    for (String url : Arrays.asList(null, "", "   ")) {
      CConfiguration cConf = CConfiguration.create();
      if (url != null) {
        cConf.set(Constants.RuntimeMonitor.MONITOR_URL, url);
      }
      Injector injector = createInjector(cConf, runId);
      IllegalStateException e = Assert.assertThrows(IllegalStateException.class,
          () -> RuntimeMonitors.setupMonitoring(injector, opts));
      Assert.assertTrue(e.getMessage().contains(Constants.RuntimeMonitor.MONITOR_URL));
      Assert.assertNull(System.getProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI));
    }
  }

  @Test
  public void testSetupMonitoringSetsRoutingBaseUri() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL, "  https://monitor.example.com  ");
    ProgramRunId runId = NamespaceId.DEFAULT.app("a").workflow("wf").run(RunIds.generate());
    Injector injector = createInjector(cConf, runId);
    ProgramOptions opts = new SimpleProgramOptions(
        runId.getParent(), new BasicArguments(), new BasicArguments());

    RuntimeMonitors.setupMonitoring(injector, opts);
    Assert.assertEquals(
        String.format(
            "https://monitor.example.com/v3Internal/runtime/namespaces/default/apps/a/"
                + "versions/-SNAPSHOT/workflows/wf/runs/%s/services/", runId.getRun()),
        System.getProperty(RemoteClient.RUNTIME_SERVICE_ROUTING_BASE_URI));
  }

  @Test
  public void testGetRemoteAuthenticatorModuleBranches() {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.RemoteAuthenticator.REMOTE_AUTHENTICATOR_NAME,
        NoOpRemoteAuthenticator.NO_OP_REMOTE_AUTHENTICATOR_NAME);
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL_AUTHENTICATOR_NAME_PREFIX + "gcp-dataproc",
        GceRemoteAuthenticator.GCE_REMOTE_AUTHENTICATOR_NAME);
    cConf.set(Constants.RuntimeMonitor.MONITOR_URL_AUTHENTICATOR_NAME_PREFIX + "null",
        GceRemoteAuthenticator.GCE_REMOTE_AUTHENTICATOR_NAME);

    ProgramId programId = NamespaceId.DEFAULT.app("a").workflow("wf");
    ProgramOptions withProvisioner = new SimpleProgramOptions(
        programId,
        new BasicArguments(Collections.singletonMap(
            SystemArguments.PROFILE_PROVISIONER, "gcp-dataproc")),
        new BasicArguments());
    ProgramOptions withoutProvisioner = new SimpleProgramOptions(
        programId, new BasicArguments(), new BasicArguments());

    // ISOLATED + provisioner -> uses provisioner-specific override
    RemoteAuthenticator isolated = Guice.createInjector(
        new ConfigModule(cConf),
        RuntimeMonitors.getRemoteAuthenticatorModule(ClusterMode.ISOLATED, withProvisioner))
        .getInstance(RemoteAuthenticator.class);
    Assert.assertEquals(GceRemoteAuthenticator.GCE_REMOTE_AUTHENTICATOR_NAME, isolated.getName());

    // ON_PREMISE + provisioner -> ignores provisioner override and uses default
    RemoteAuthenticator onPrem = Guice.createInjector(
        new ConfigModule(cConf),
        RuntimeMonitors.getRemoteAuthenticatorModule(ClusterMode.ON_PREMISE, withProvisioner))
        .getInstance(RemoteAuthenticator.class);
    Assert.assertEquals(NoOpRemoteAuthenticator.NO_OP_REMOTE_AUTHENTICATOR_NAME, onPrem.getName());

    // ISOLATED + null provisioner -> does not look up "...null", uses default
    RemoteAuthenticator noProvisioner = Guice.createInjector(
        new ConfigModule(cConf),
        RuntimeMonitors.getRemoteAuthenticatorModule(ClusterMode.ISOLATED, withoutProvisioner))
        .getInstance(RemoteAuthenticator.class);
    Assert.assertEquals(NoOpRemoteAuthenticator.NO_OP_REMOTE_AUTHENTICATOR_NAME,
        noProvisioner.getName());
  }

  private static Injector createInjector(CConfiguration cConf, ProgramRunId runId) {
    return Guice.createInjector(new ConfigModule(cConf), new AbstractModule() {
      @Override
      protected void configure() {
        bind(ProgramRunId.class).toInstance(runId);
      }
    });
  }
}
