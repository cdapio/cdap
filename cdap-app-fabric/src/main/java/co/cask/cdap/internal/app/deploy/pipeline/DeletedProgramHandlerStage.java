/*
 * Copyright © 2014-2016 Cask Data, Inc.
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
import co.cask.cdap.api.metrics.MetricDeleteQuery;
import co.cask.cdap.api.metrics.MetricStore;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.security.Impersonator;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.data2.transaction.queue.QueueAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConsumerFactory;
import co.cask.cdap.internal.app.deploy.ProgramTerminator;
import co.cask.cdap.internal.app.runtime.flow.FlowUtils;
import co.cask.cdap.pipeline.AbstractStage;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.ProgramTypes;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramId;
import co.cask.cdap.security.spi.authorization.PrivilegesManager;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Deleted program handler stage. Figures out which programs are deleted and handles callback.
 */
public class DeletedProgramHandlerStage extends AbstractStage<ApplicationDeployable> {

  private static final Logger LOG = LoggerFactory.getLogger(DeletedProgramHandlerStage.class);

  private final Store store;
  private final ProgramTerminator programTerminator;
  private final StreamConsumerFactory streamConsumerFactory;
  private final QueueAdmin queueAdmin;
  private final MetricStore metricStore;
  private final MetadataStore metadataStore;
  private final PrivilegesManager privilegesManager;
  private final Impersonator impersonator;

  public DeletedProgramHandlerStage(Store store, ProgramTerminator programTerminator,
                                    StreamConsumerFactory streamConsumerFactory,
                                    QueueAdmin queueAdmin, MetricStore metricStore,
                                    MetadataStore metadataStore, PrivilegesManager privilegesManager,
                                    Impersonator impersonator) {
    super(TypeToken.of(ApplicationDeployable.class));
    this.store = store;
    this.programTerminator = programTerminator;
    this.streamConsumerFactory = streamConsumerFactory;
    this.queueAdmin = queueAdmin;
    this.metricStore = metricStore;
    this.metadataStore = metadataStore;
    this.privilegesManager = privilegesManager;
    this.impersonator = impersonator;
  }

  @Override
  public void process(ApplicationDeployable appSpec) throws Exception {
    List<ProgramSpecification> deletedSpecs = store.getDeletedProgramSpecifications(appSpec.getApplicationId(),
                                                                                    appSpec.getSpecification());

    // TODO: this should also delete logs and run records (or not?), and do it for all program types [CDAP-2187]

    List<String> deletedFlows = Lists.newArrayList();
    for (ProgramSpecification spec : deletedSpecs) {
      //call the deleted spec
      ProgramType type = ProgramTypes.fromSpecification(spec);
      final ProgramId programId = appSpec.getApplicationId().program(type, spec.getName());
      programTerminator.stop(programId);
      // revoke privileges
      privilegesManager.revoke(programId);

      // TODO: Unify with AppFabricHttpHandler.removeApplication
      // drop all queues and stream states of a deleted flow
      if (ProgramType.FLOW.equals(type)) {
        FlowSpecification flowSpecification = (FlowSpecification) spec;

        // Collects stream name to all group ids consuming that stream
        final Multimap<String, Long> streamGroups = HashMultimap.create();
        for (FlowletConnection connection : flowSpecification.getConnections()) {
          if (connection.getSourceType() == FlowletConnection.Type.STREAM) {
            long groupId = FlowUtils.generateConsumerGroupId(programId, connection.getTargetName());
            streamGroups.put(connection.getSourceName(), groupId);
          }
        }
        // Remove all process states and group states for each stream
        final String namespace = String.format("%s.%s", programId.getApplication(), programId.getProgram());

        final NamespaceId namespaceId = appSpec.getApplicationId().getParent();
        impersonator.doAs(namespaceId, new Callable<Void>() {

          @Override
          public Void call() throws Exception {
            for (Map.Entry<String, Collection<Long>> entry : streamGroups.asMap().entrySet()) {
              streamConsumerFactory.dropAll(namespaceId.stream(entry.getKey()).toId(), namespace, entry.getValue());
            }
            queueAdmin.dropAllForFlow(Id.Flow.from(programId.getApplication(), programId.getProgram()));
            return null;
          }
        });
        deletedFlows.add(programId.getProgram());
      }

      // Remove metadata for the deleted program
      metadataStore.removeMetadata(programId.toId());
    }
    if (!deletedFlows.isEmpty()) {
      deleteMetrics(appSpec.getApplicationId(), deletedFlows);
    }

    emit(appSpec);
  }

  private void deleteMetrics(ApplicationId applicationId, Iterable<String> flows) throws Exception {
    LOG.debug("Deleting metrics for application {}", applicationId);
    for (String flow : flows) {
      long endTs = System.currentTimeMillis() / 1000;
      Map<String, String> tags = Maps.newHashMap();
      tags.put(Constants.Metrics.Tag.NAMESPACE, applicationId.getNamespace());
      tags.put(Constants.Metrics.Tag.APP, applicationId.getApplication());
      tags.put(Constants.Metrics.Tag.FLOW, flow);
      MetricDeleteQuery deleteQuery = new MetricDeleteQuery(0, endTs, Collections.<String>emptyList(), tags);
      metricStore.delete(deleteQuery);
    }
  }
}
