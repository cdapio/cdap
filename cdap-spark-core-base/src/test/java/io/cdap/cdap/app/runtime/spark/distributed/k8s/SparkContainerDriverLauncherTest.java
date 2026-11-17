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

package io.cdap.cdap.app.runtime.spark.distributed.k8s;

import com.google.common.util.concurrent.Service;
import com.google.inject.Injector;
import io.cdap.cdap.app.runtime.spark.Constant;
import io.cdap.cdap.app.runtime.spark.MockMasterEnvironment;
import io.cdap.cdap.app.runtime.spark.service.ArtifactFetcherService;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.http.CommonNettyHttpServiceFactory;
import io.cdap.cdap.master.environment.MasterEnvironments;
import io.cdap.cdap.master.spi.environment.MasterEnvironment;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.http.NettyHttpService;
import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for SparkContainerDriverLauncher.
 */
public class SparkContainerDriverLauncherTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testInjectorSuccessfullyInjectsServiceAndStartsArtifactFetcher() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.setInt(Constant.Spark.ArtifactFetcher.PORT, 0);
    Configuration hConf = new Configuration();
    MasterEnvironment mockMasterEnvironment = new MockMasterEnvironment();
    MasterEnvironmentContext context = MasterEnvironments.createContext(cConf, hConf, "mock");
    mockMasterEnvironment.initialize(context);
    Injector injector = SparkContainerDriverLauncher.createInjector(cConf, hConf, mockMasterEnvironment);
    CommonNettyHttpServiceFactory nettyFactory = injector.getInstance(CommonNettyHttpServiceFactory.class);

    File workDir = TEMP_FOLDER.newFolder();
    Files.write(new File(workDir, "sample.txt").toPath(), "bundle-data".getBytes(StandardCharsets.UTF_8));

    Method createBundleMethod = SparkContainerDriverLauncher.class.getDeclaredMethod("createBundle", Path.class);
    createBundleMethod.setAccessible(true);
    Location bundleLocation = (Location) createBundleMethod.invoke(null, workDir.toPath());

    ArtifactFetcherService fetcherService = new ArtifactFetcherService(cConf, bundleLocation, nettyFactory);
    fetcherService.startAsync().awaitRunning();
    try {
      Assert.assertTrue(fetcherService.isRunning());
      Field httpServiceField = ArtifactFetcherService.class.getDeclaredField("httpService");
      httpServiceField.setAccessible(true);
      NettyHttpService httpService = (NettyHttpService) httpServiceField.get(fetcherService);
      int boundPort = httpService.getBindAddress().getPort();
      URL fetchUrl = new URL("http://127.0.0.1:" + boundPort + Constants.Gateway.INTERNAL_API_VERSION_3
                             + "/artifacts/fetch");
      HttpURLConnection conn = (HttpURLConnection) fetchUrl.openConnection();
      try {
        Assert.assertEquals(200, conn.getResponseCode());
      } finally {
        conn.disconnect();
      }
    } finally {
      fetcherService.stopAsync().awaitTerminated();
    }
    Assert.assertEquals(Service.State.TERMINATED, fetcherService.state());
  }
}
