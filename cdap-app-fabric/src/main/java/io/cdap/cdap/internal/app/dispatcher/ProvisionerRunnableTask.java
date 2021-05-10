/*
 * Copyright © 2021 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.internal.app.dispatcher;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.codec.CConfigurationCodec;
import io.cdap.cdap.internal.provision.task.SubtaskConfig;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.ssh.SSHContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Task to run provisoner stuff
 */
public class ProvisionerRunnableTask extends RunnableTask {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisionerRunnableTask.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(CConfiguration.class, new CConfigurationCodec()).create();

  @Override
  protected byte[] run(String param) throws Exception {
    LOG.error("Got to the provisioner task in the runner with params: " + param);
    SubtaskConfig config = GSON.fromJson(param, SubtaskConfig.class);
    Provisioner provisioner = config.getProvisioner();
    ProvisionerContext provisionerContext = config.getProvisionerContext();
    Cluster nextCluster = provisioner.createCluster(provisionerContext);
    SSHContext sshContext = provisionerContext.getSSHContext();
    // ssh context can be null if ssh is not being used to submit job
    if (sshContext == null) {
      return toBytes(new Cluster(nextCluster.getName(), nextCluster.getStatus(), nextCluster.getNodes(),
                                 nextCluster.getProperties()));
    }

    Map<String, String> properties = new HashMap<>(nextCluster.getProperties());
    // Set the SSH user if the provisioner sets it.
    provisionerContext.getSSHContext().getSSHKeyPair().ifPresent(sshKeyPair -> {
      properties.put(Constants.RuntimeMonitor.SSH_USER, sshKeyPair.getPublicKey().getUser());
    });
    return toBytes(new Cluster(nextCluster.getName(), nextCluster.getStatus(), nextCluster.getNodes(), properties));
  }

  private byte[] toBytes(Object obj) {
    String json = GSON.toJson(obj);
    return json.getBytes();
  }

  @Override
  protected void startUp() throws Exception {

  }

  @Override
  protected void shutDown() throws Exception {

  }
}
