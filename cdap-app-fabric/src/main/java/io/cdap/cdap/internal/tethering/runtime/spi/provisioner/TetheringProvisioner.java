/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.internal.tethering.runtime.spi.provisioner;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.internal.provision.ProvisionerExtensionLoader;
import io.cdap.cdap.internal.tethering.TetheringStore;
import io.cdap.cdap.internal.tethering.runtime.spi.runtimejob.TetheringRuntimeJobManager;
import io.cdap.cdap.messaging.spi.MessagingService;
import io.cdap.cdap.runtime.spi.provisioner.Capabilities;
import io.cdap.cdap.runtime.spi.provisioner.Cluster;
import io.cdap.cdap.runtime.spi.provisioner.ClusterStatus;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategies;
import io.cdap.cdap.runtime.spi.provisioner.PollingStrategy;
import io.cdap.cdap.runtime.spi.provisioner.Provisioner;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerContext;
import io.cdap.cdap.runtime.spi.provisioner.ProvisionerSpecification;
import io.cdap.cdap.runtime.spi.runtimejob.RuntimeJobManager;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import org.apache.twill.filesystem.LocationFactory;

/**
 * A provisioner that does not create or tear down clusters, but just uses an existing tethered CDAP
 * instance.
 */
public class TetheringProvisioner implements Provisioner {

  public static final String TETHERING_NAME = "tethering";
  private static final ProvisionerSpecification SPEC = new ProvisionerSpecification(
      TETHERING_NAME, "Tethering",
      "Runs programs on an existing tethered CDAP instance. Does not provision any resources.");

  private MessagingService messagingService;
  private TetheringStore tetheringStore;
  private LocationFactory locationFactory;
  private CConfiguration cConf;

  /**
   * Using method injection instead of constructor injection because {@link ServiceLoader} (used by
   * {@link ProvisionerExtensionLoader}) requires no-argument constructor for instantiation.
   */
  @Inject
  @SuppressWarnings("unused")
  void setMessagingService(MessagingService messagingService) {
    this.messagingService = messagingService;
  }

  @Inject
  @SuppressWarnings("unused")
  void setTetheringStore(TetheringStore tetheringStore) {
    this.tetheringStore = tetheringStore;
  }

  @Inject
  @SuppressWarnings("unused")
  void setLocationFactory(LocationFactory locationFactory) {
    this.locationFactory = locationFactory;
  }

  @Inject
  void setCConf(CConfiguration cConf) {
    this.cConf = cConf;
  }

  @Override
  public ProvisionerSpecification getSpec() {
    return SPEC;
  }

  @Override
  public void validateProperties(Map<String, String> properties) {
    // Creates the TetheringConf for validation
    TetheringConf.fromProperties(properties);
  }

  @Override
  public Cluster createCluster(ProvisionerContext context) {
    return new Cluster(context.getProgramRunInfo().getRun(), ClusterStatus.RUNNING,
        Collections.emptyList(), Collections.emptyMap());
  }

  @Override
  public ClusterStatus getClusterStatus(ProvisionerContext context, Cluster cluster) {
    ClusterStatus status = cluster.getStatus();
    return status == ClusterStatus.DELETING ? ClusterStatus.NOT_EXISTS : status;
  }

  @Override
  public Cluster getClusterDetail(ProvisionerContext context, Cluster cluster) {
    return new Cluster(cluster, getClusterStatus(context, cluster));
  }

  @Override
  public void deleteCluster(ProvisionerContext context, Cluster cluster) {
    // no-op
  }

  @Override
  public PollingStrategy getPollingStrategy(ProvisionerContext context, Cluster cluster) {
    // shouldn't matter, as we won't ever poll
    return PollingStrategies.fixedInterval(0, TimeUnit.SECONDS);
  }

  @Override
  public Capabilities getCapabilities() {
    return Capabilities.EMPTY;
  }

  /**
   * Provides implementation of {@link RuntimeJobManager}.
   */
  @Override
  public Optional<RuntimeJobManager> getRuntimeJobManager(ProvisionerContext context) {
    TetheringConf conf = TetheringConf.fromProperties(context.getProperties());
    return Optional.of(new TetheringRuntimeJobManager(conf, cConf, messagingService, tetheringStore,
        locationFactory));
  }
}
