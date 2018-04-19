/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.provision;

import co.cask.cdap.api.Transactional;
import co.cask.cdap.api.Transactionals;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.SSHKeyPair;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.KeyPair;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.TimeUnit;

/**
 * Performs steps to provision a cluster for a program run.
 */
public class ProvisionTask extends ProvisioningTask {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisionTask.class);
  private final ProvisionRequest provisionRequest;
  private final Provisioner provisioner;
  private final ProvisionerContext provisionerContext;
  private final ProvisionerNotifier provisionerNotifier;
  private final LocationFactory locationFactory;
  private final Transactional transactional;
  private final DatasetFramework datasetFramework;

  public ProvisionTask(ProvisionRequest provisionRequest, Provisioner provisioner,
                       ProvisionerContext provisionerContext, ProvisionerNotifier provisionerNotifier,
                       LocationFactory locationFactory,
                       Transactional transactional, DatasetFramework datasetFramework) {
    super(provisionRequest.getProgramRunId());
    this.provisionRequest = provisionRequest;
    this.provisioner = provisioner;
    this.provisionerContext = provisionerContext;
    this.provisionerNotifier = provisionerNotifier;
    this.locationFactory = locationFactory;
    this.transactional = transactional;
    this.datasetFramework = datasetFramework;
  }

  @Override
  public void run() {
    ClusterInfo existing = Transactionals.execute(transactional, datasetContext -> {
      ProvisionerDataset provisionerDataset = ProvisionerDataset.get(datasetContext, datasetFramework);
      return provisionerDataset.getClusterInfo(programRunId);
    });

    if (existing == null) {
      LOG.error("Received request to provision a cluster for program run {}, but could not find information " +
                  "about the program run.", programRunId);
      // TODO: CDAP-13246 move to orphaned state
      return;
    }

    try {
      SSHKeyInfo sshKeyInfo = generateSSHKey(programRunId);
      SSHKeyPair sshKeyPair =
        new SSHKeyPair(sshKeyInfo.getUsername(), sshKeyInfo.getPublicKey(), sshKeyInfo.getPrivateKey());
      ProvisionerContext contextWithKey = new DefaultProvisionerContext(programRunId,
                                                                        provisionerContext.getProperties(),
                                                                        sshKeyPair);
      Cluster cluster = provisioner.createCluster(contextWithKey);
      if (cluster == null) {
        // this is in violation of the provisioner contract, but in case somebody writes a provisioner that
        // returns a null cluster.
        provisionerNotifier.deprovisioning(programRunId);
        return;
      }
      ClusterOp op = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.POLLING_CREATE);
      final ClusterInfo pollingInfo = new ClusterInfo(existing, op, sshKeyInfo, cluster);

      Transactionals.execute(transactional, dsContext -> {
        ProvisionerDataset dataset = ProvisionerDataset.get(dsContext, datasetFramework);
        dataset.putClusterInfo(pollingInfo);
      });

      while (cluster.getStatus() == ClusterStatus.CREATING) {
        cluster = provisioner.getClusterDetail(provisionerContext, cluster);
        TimeUnit.SECONDS.sleep(10);
      }

      // TODO: CDAP-13246 handle unexpected states and retry
      switch (cluster.getStatus()) {
        case RUNNING:
          provisioner.initializeCluster(provisionerContext, cluster);
          op = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.CREATED);
          final ClusterInfo runningInfo = new ClusterInfo(pollingInfo, op, pollingInfo.getSshKeyInfo(), cluster);
          Transactionals.execute(transactional, dsContext -> {
            ProvisionerDataset dataset = ProvisionerDataset.get(dsContext, datasetFramework);
            dataset.putClusterInfo(runningInfo);
          });
          provisionerNotifier.provisioned(programRunId, provisionRequest.getProgramOptions(),
                                          provisionRequest.getProgramDescriptor(), provisionRequest.getUser(),
                                          cluster, sshKeyInfo);
          break;
      }
    } catch (Throwable t) {
      // TODO: CDAP-13246 handle retries
      LOG.warn("Error provisioning cluster for program run {}", programRunId, t);
      provisionerNotifier.deprovisioning(programRunId);
    }
  }

  /**
   * Generates a SSH key pair.
   */
  private SSHKeyInfo generateSSHKey(ProgramRunId programRunId) throws JSchException, IOException {
    JSch jsch = new JSch();
    KeyPair keyPair = KeyPair.genKeyPair(jsch, KeyPair.RSA, 2048);

    Location keysDir = locationFactory.create(String.format("provisioner/keys/%s.%s.%s.%s.%s",
                                                            programRunId.getNamespace(),
                                                            programRunId.getApplication(),
                                                            programRunId.getType().name().toLowerCase(),
                                                            programRunId.getProgram(),
                                                            programRunId.getRun()));
    keysDir.mkdirs();

    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    keyPair.writePublicKey(bos, "cdap@cask.co");
    byte[] publicKey = bos.toByteArray();

    Location publicKeyFile = keysDir.append("id_rsa.pub");
    try (OutputStream os = publicKeyFile.getOutputStream()) {
      os.write(publicKey);
    }

    bos = new ByteArrayOutputStream();
    keyPair.writePrivateKey(bos, null);
    byte[] privateKey = bos.toByteArray();

    Location privateKeyFile = keysDir.append("id_rsa");
    try (OutputStream os = privateKeyFile.getOutputStream("600")) {
      os.write(privateKey);
    }

    return new SSHKeyInfo(publicKeyFile.toURI(), privateKeyFile.toURI(),
                          publicKey, privateKey, "cdap");
  }

}
