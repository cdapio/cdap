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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobEnvironment;
import org.apache.twill.zookeeper.ZKClientService;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Map;

/**
 * Unit test for {@link RemoteExecutionJobMain}.
 */
public class RemoteExecutionJobMainTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testJobEnvironment() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder().getAbsolutePath());
    cConf.set(Constants.CFG_HDFS_NAMESPACE, TEMP_FOLDER.newFolder().getAbsolutePath());

    File secretFile = new File(TEMP_FOLDER.newFolder(), Constants.RuntimeMonitor.SERVICE_PROXY_PASSWORD_FILE);
    Files.write(secretFile.toPath(), Collections.singletonList("testing"));

    RemoteExecutionJobMain runner = new RemoteExecutionJobMain();
    runner.setServiceProxySecretPath(secretFile.toPath());
    RuntimeJobEnvironment jobEnv = runner.initialize(cConf);
    try {
      Assert.assertNotNull(jobEnv.getLocationFactory());
      Assert.assertNotNull(jobEnv.getTwillRunner());

      Map<String, String> properties = jobEnv.getProperties();
      ZKClientService zkClient = ZKClientService.Builder.of(properties.get(Constants.Zookeeper.QUORUM)).build();
      zkClient.startAndWait();
      try {
        Assert.assertNotNull(zkClient.exists("/").get());
      } finally {
        zkClient.stopAndWait();
      }
    } finally {
      runner.destroy();
    }
  }
}
