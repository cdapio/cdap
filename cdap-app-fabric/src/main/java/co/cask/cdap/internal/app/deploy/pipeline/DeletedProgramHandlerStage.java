/*
 * Copyright © 2014-2015 Cask Data, Inc.
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

package co.cask.cdap.internal.app.deploy.pipeline;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import com.google.common.base.Throwables;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Deleted program handler stage. Figures out which programs are deleted and handles callback.
 */
public class DeletedProgramHandlerStage extends AbstractStage<ApplicationDeployable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeletedProgramHandlerStage.class);

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  /**
   * Timeout to get response from metrics system.
   */
  private static final long METRICS_SERVER_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

  private final Store store;
  private final ProgramTerminator programTerminator;
  private final StreamConsumerFactory streamConsumerFactory;
  private final QueueAdmin queueAdmin;
  private final DiscoveryServiceClient discoveryServiceClient;

  public DeletedProgramHandlerStage(Store store, ProgramTerminator programTerminator,
                                    StreamConsumerFactory streamConsumerFactory,
                                    QueueAdmin queueAdmin, DiscoveryServiceClient discoveryServiceClient) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.store = store;
    this.programTerminator = programTerminator;
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public void process(ApplicationDeployable appSpec) throws Exception {
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appSpec.getId(),
                                                                                    appSpec.getSpecification());

    List<String> deletedFlows = Lists.newArrayList();
    for (ProgramSpecification spec : deletedSpecs) {
      //call the deleted spec
      ProgramType type = ProgramTypes.fromSpecification(spec);
      Id.Program programId = Id.Program.from(appSpec.getId(), type, spec.getName());
      programTerminator.stop(Id.Namespace.from(appSpec.getId().getNamespaceId()),
                             programId, type);

      // TODO: Unify with AppFabricHttpHandler.removeApplication
      // drop all queues and stream states of a deleted flow
      if (ProgramType.FLOW.equals(type)) {
        FlowSpecification flowSpecification = (FlowSpecification) spec;

        // Collects stream name to all group ids consuming that stream
        Multimap<String, Long> streamGroups = HashMultimap.create();
        for (FlowletConnection connection : flowSpecification.getConnections()) {
          if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
            long groupId = FlowUtils.generateConsumerGroupId(programId, connection.getTargetName());
            streamGroups.put(connection.getSourceName(), groupId);
          }
        }
        // Remove all process states and group states for each stream
        String namespace = String.format("%s.%s", programId.getApplicationId(), programId.getId());
        for (Map.Entry<String, Collection<Long>> entry : streamGroups.asMap().entrySet()) {
          streamConsumerFactory.dropAll(Id.Stream.from(appSpec.getId().getNamespaceId(), entry.getKey()),
                                        namespace, entry.getValue());
        }

        queueAdmin.dropAllForFlow(programId.getNamespaceId(), programId.getApplicationId(), programId.getId());
        deletedFlows.add(programId.getId());
      }
    }
    if (!deletedFlows.isEmpty()) {
      deleteMetrics(appSpec.getId().getNamespaceId(), appSpec.getId().getId(), deletedFlows);
    }

    emit(appSpec);
  }

  private void deleteMetrics(String account, String application, Iterable<String> flows) throws IOException {
    ServiceDiscovered discovered = discoveryServiceClient.discover(Constants.Service.METRICS);
    Discoverable discoverable = new RandomEndpointStrategy(discovered).pick(DISCOVERY_TIMEOUT_SECONDS,
                                                                            TimeUnit.SECONDS);

    if (discoverable == null) {
      LOG.error("Fail to get any metrics endpoint for deleting metrics.");
      return;
    }

    LOG.debug("Deleting metrics for application {}", application);
    for (String flow : flows) {
      String url = String.format("http://%s:%d%s/metrics/%s/apps/%s/flows/%s",
                                 discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort(),
                                 Constants.Gateway.API_VERSION_2,
                                 "ignored",
                                 application, flow);

      SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
        .setUrl(url)
        .setRequestTimeoutInMs((int) METRICS_SERVER_RESPONSE_TIMEOUT)
        .build();

      try {
        client.delete().get(METRICS_SERVER_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        LOG.error("exception making metrics delete call", e);
        Throwables.propagate(e);
      } finally {
        client.close();
      }
    }
  }
}
