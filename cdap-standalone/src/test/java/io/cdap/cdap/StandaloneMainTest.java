/*
 * Copyright © 2016-2023 Cask Data, Inc.
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

package io.cdap.cdap;

import com.google.common.util.concurrent.Service;
import io.cdap.cdap.app.preview.PreviewHttpServer;
import io.cdap.cdap.app.preview.PreviewRunnerManager;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.gateway.router.NettyRouter;
import io.cdap.cdap.internal.app.preview.PreviewRunStopper;
import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TransactionManager;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for {@link StandaloneMain}.
 */
public class StandaloneMainTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testInjector() {
    StandaloneMain sdk = StandaloneMain.create(CConfiguration.create(), new Configuration());
    PreviewHttpServer previewHttpServer = sdk.getInjector().getInstance(PreviewHttpServer.class);
    PreviewRunnerManager previewRunnerManager =
        sdk.getInjector().getInstance(PreviewRunnerManager.class);
    PreviewRunStopper previewRunStopper = sdk.getInjector().getInstance(PreviewRunStopper.class);

    Assert.assertSame(previewRunnerManager, previewRunStopper);
    TransactionManager txManager = sdk.getInjector().getInstance(TransactionManager.class);
    txManager.startAndWait();
    previewHttpServer.startAndWait();
    ((Service) previewRunnerManager).startAndWait();
    ((Service) previewRunnerManager).stopAndWait();
    previewHttpServer.stopAndWait();
    txManager.stopAndWait();
  }

  @Test
  public void testStartUpAndShutDownLifecycle() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    String loopback = InetAddress.getLoopbackAddress().getHostAddress();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, tempFolder.newFolder("standalone").getAbsolutePath());
    cConf.setBoolean(StandaloneMain.DISABLE_UI, true);
    cConf.set(Constants.Router.ADDRESS, loopback);
    cConf.setInt(Constants.Router.ROUTER_PORT, 0);
    cConf.set(Constants.Preview.ADDRESS, loopback);
    cConf.setInt(Constants.Preview.PORT, 0);
    cConf.setInt(Constants.ArtifactLocalizer.PORT, 0);

    StandaloneMain sdk = StandaloneMain.create(cConf, new Configuration());
    sdk.startUp();
    try {
      NettyRouter router = sdk.getInjector().getInstance(NettyRouter.class);
      Assert.assertEquals(Service.State.RUNNING, router.state());
    } finally {
      sdk.shutDown();
    }
    NettyRouter router = sdk.getInjector().getInstance(NettyRouter.class);
    Assert.assertEquals(Service.State.TERMINATED, router.state());
  }
}
