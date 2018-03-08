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

import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.internal.app.runtime.SystemArguments;
import co.cask.cdap.proto.id.ProgramRunId;
import co.cask.cdap.proto.provisioner.ProvisionerDetail;
import co.cask.cdap.runtime.spi.provisioner.Cluster;
import co.cask.cdap.runtime.spi.provisioner.ClusterStatus;
import co.cask.cdap.runtime.spi.provisioner.Provisioner;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerContext;
import co.cask.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Service for provisioning related operations
 */
public class ProvisioningService extends AbstractIdleService {
  private static final Logger LOG = LoggerFactory.getLogger(ProvisioningService.class);
  private final AtomicReference<ProvisionerInfo> provisionerInfo;
  private final ProvisionerProvider provisionerProvider;
  private final ProvisionerConfigProvider provisionerConfigProvider;
  private final ProvisionerStore provisionerStore;
  private final ProvisionerNotifier provisionerNotifier;
  private final ExecutorService executorService;

  @Inject
  ProvisioningService(ProvisionerProvider provisionerProvider, ProvisionerConfigProvider provisionerConfigProvider,
                      ProvisionerStore provisionerStore, ProvisionerNotifier provisionerNotifier) {
    this.provisionerProvider = provisionerProvider;
    this.provisionerConfigProvider = provisionerConfigProvider;
    this.provisionerStore = provisionerStore;
    this.provisionerNotifier = provisionerNotifier;
    this.executorService = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setNameFormat("provisioning-service").build());
    this.provisionerInfo = new AtomicReference<>(new ProvisionerInfo(new HashMap<>(), new HashMap<>()));
    reloadProvisioners();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.info("Starting {}", getClass().getSimpleName());
    // TODO: CDAP-13246 check ProvisionerDataset to find any operations that are supposed to be in progress and
    // pick up where they left off
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.info("Stopping {}", getClass().getSimpleName());
    try {
      // Shutdown the executor, which will issue an interrupt to the running thread.
      executorService.shutdownNow();
      executorService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // Ignore it.
    } finally {
      if (!executorService.isTerminated()) {
        executorService.shutdownNow();
      }
    }
    LOG.info("Stopped {}", getClass().getSimpleName());
  }

  public void provision(ProvisionRequest provisionRequest) {
    ProgramRunId programRunId = provisionRequest.getProgramRunId();
    ProgramOptions programOptions = provisionRequest.getProgramOptions();
    Map<String, String> args = programOptions.getUserArguments().asMap();
    String name = SystemArguments.getProfileProvisioner(args);
    Provisioner provisioner = provisionerInfo.get().provisioners.get(name);
    if (provisioner == null) {
      LOG.error("Could not provision cluster for program run {} because provisioner {} does not exist.",
                programRunId, name);
      provisionerNotifier.deprovisioned(programRunId);
      return;
    }

    Map<String, String> properties = SystemArguments.getProfileProperties(args);
    ProvisionerContext context = new DefaultProvisionerContext(programRunId, properties);

    ClusterOp clusterOp = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.REQUESTING_CREATE);
    ClusterInfo clusterInfo =
      new ClusterInfo(programRunId, provisionRequest.getProgramDescriptor(),
                      properties, name, provisionRequest.getUser(), clusterOp, null);

    provisionerStore.putClusterInfo(clusterInfo);

    executorService.execute(() -> {
      try {
        Cluster cluster = provisioner.createCluster(context);
        if (cluster == null) {
          // this is in violation of the provisioner contract, but in case somebody writes a provisioner that
          // returns a null cluster.
          provisionerNotifier.deprovisioning(programRunId);
          return;
        }
        ClusterOp op = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.POLLING_CREATE);
        ClusterInfo info = new ClusterInfo(clusterInfo, op, cluster);
        provisionerStore.putClusterInfo(info);
        ClusterStatus status = cluster.getStatus();

        while (status == ClusterStatus.CREATING) {
          status = provisioner.getClusterStatus(context, cluster);
          TimeUnit.SECONDS.sleep(10);
        }

        // TODO: CDAP-13246 handle unexpected states and retry
        switch (status) {
          case RUNNING:
            cluster = new Cluster(cluster.getName(), ClusterStatus.RUNNING,
                                  cluster.getNodes(), cluster.getProperties());
            op = new ClusterOp(ClusterOp.Type.PROVISION, ClusterOp.Status.CREATED);
            info = new ClusterInfo(info, op, cluster);
            provisionerStore.putClusterInfo(info);
            provisionerNotifier.provisioned(programRunId, programOptions, provisionRequest.getProgramDescriptor(),
                                            provisionRequest.getUser(), cluster);
            break;
        }
      } catch (Throwable t) {
        // TODO: CDAP-13246 handle retries
        LOG.warn("Error provisioning cluster for program run {}", programRunId, t);
        provisionerNotifier.deprovisioning(programRunId);
      }
    });
  }

  public void deprovision(ProgramRunId programRunId) {
    ClusterInfo existing = provisionerStore.getClusterInfo(programRunId);
    if (existing == null) {
      LOG.error("Received request to de-provision a cluster for program run {}, but could not find information " +
                  "about the cluster.", programRunId);
      // TODO: CDAP-13246 move to orphaned state
      return;
    }
    Provisioner provisioner = provisionerInfo.get().provisioners.get(existing.getProvisionerName());
    if (provisioner == null) {
      LOG.error("Could not de-provision cluster for program run {} because provisioner {} does not exist.",
                programRunId, existing.getProvisionerName());
      // TODO: CDAP-13246 move to orphaned state
      return;
    }

    Map<String, String> properties = existing.getProvisionerProperties();
    ProvisionerContext context = new DefaultProvisionerContext(programRunId, properties);

    ClusterOp clusterOp = new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.REQUESTING_DELETE);
    ClusterInfo clusterInfo = new ClusterInfo(existing, clusterOp, existing.getCluster());

    provisionerStore.putClusterInfo(clusterInfo);

    executorService.execute(() -> {
      try {
        provisioner.deleteCluster(context, clusterInfo.getCluster());
        ClusterOp op = new ClusterOp(ClusterOp.Type.DEPROVISION, ClusterOp.Status.POLLING_DELETE);
        Cluster cluster = new Cluster(clusterInfo.getCluster().getName(), ClusterStatus.DELETING,
                                      clusterInfo.getCluster().getNodes(), clusterInfo.getCluster().getProperties());
        ClusterInfo info = new ClusterInfo(clusterInfo, op, cluster);
        provisionerStore.putClusterInfo(info);

        ClusterStatus status = ClusterStatus.DELETING;
        while (status == ClusterStatus.DELETING) {
          status = provisioner.getClusterStatus(context, cluster);
          TimeUnit.SECONDS.sleep(10);
        }

        // TODO: CDAP-13246 handle unexpected states and retry
        switch (status) {
          case NOT_EXISTS:
            provisionerNotifier.deprovisioned(programRunId);
            provisionerStore.deleteClusterInfo(programRunId);
            break;
        }
      } catch (Throwable t) {
        LOG.warn("Error deprovisioning cluster for program run {}", programRunId, t);
        // TODO: CDAP-13246 handle retries
      }
    });
  }

  /**
   * Reloads provisioners in the extension directory. Any new provisioners will be added and any deleted provisioners
   * will be removed.
   */
  public void reloadProvisioners() {
    Map<String, Provisioner> provisioners = provisionerProvider.loadProvisioners();
    Map<String, ProvisionerConfig> provisionerConfigs =
      provisionerConfigProvider.loadProvisionerConfigs(provisioners.keySet());
    LOG.debug("Provisioners = {}", provisioners);
    Map<String, ProvisionerDetail> details = new HashMap<>(provisioners.size());
    for (Map.Entry<String, Provisioner> provisionerEntry : provisioners.entrySet()) {
      ProvisionerSpecification spec = provisionerEntry.getValue().getSpec();
      String provisionerName = provisionerEntry.getKey();
      ProvisionerConfig config = provisionerConfigs.getOrDefault(provisionerName,
                                                                 new ProvisionerConfig(new ArrayList<>()));
      details.put(provisionerName, new ProvisionerDetail(spec.getName(), spec.getDescription(),
                                                         config.getConfigurationGroups()));
    }
    provisionerInfo.set(new ProvisionerInfo(provisioners, details));
  }

  /**
   * @return unmodifiable collection of all provisioner specs
   */
  public Collection<ProvisionerDetail> getProvisionerDetails() {
    return provisionerInfo.get().details.values();
  }

  /**
   * Get the spec for the specified provisioner.
   *
   * @param name the name of the provisioner
   * @return the spec for the provisioner, or null if the provisioner does not exist
   */
  @Nullable
  public ProvisionerDetail getProvisionerDetail(String name) {
    return provisionerInfo.get().details.get(name);
  }

  /**
   * Validate properties for the specified provisioner.
   *
   * @param provisionerName the name of the provisioner to validate
   * @param properties properties for the specified provisioner
   * @throws NotFoundException if the provisioner does not exist
   * @throws IllegalArgumentException if the properties are invalid
   */
  public void validateProperties(String provisionerName, Map<String, String> properties) throws NotFoundException {
    Provisioner provisioner = provisionerInfo.get().provisioners.get(provisionerName);
    if (provisioner == null) {
      throw new NotFoundException(String.format("Provisioner '%s' does not exist", provisionerName));
    }
    provisioner.validateProperties(properties);
  }

  /**
   * Just a container for provisioner instances and specs, so that they can be updated atomically.
   */
  private static class ProvisionerInfo {
    private final Map<String, Provisioner> provisioners;
    private final Map<String, ProvisionerDetail> details;

    private ProvisionerInfo(Map<String, Provisioner> provisioners, Map<String, ProvisionerDetail> details) {
      this.provisioners = Collections.unmodifiableMap(provisioners);
      this.details = Collections.unmodifiableMap(details);
    }
  }
}
