/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.queue;

import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.internal.io.ReflectionSchemaGenerator;
import co.cask.cdap.proto.id.ApplicationId;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.Map;
import java.util.Set;

/**
 * Concrete implementation of {@link QueueSpecificationGenerator} for generating queue
 * names.
 */
public final class SimpleQueueSpecificationGenerator extends AbstractQueueSpecificationGenerator {

  private static final Schema STREAM_EVENT_SCHEMA;

  static {
    Schema schema;
    try {
      schema = new ReflectionSchemaGenerator().generate(StreamEvent.class);
    } catch (UnsupportedTypeException e) {
      schema = null;
    }
    STREAM_EVENT_SCHEMA = schema;
  }

  /**
   * Account Name under which the stream names to generated.
   */
  private final ApplicationId appId;

  /**
   * Constructor that takes an appId.
   *
   * @param appId under which the stream is represented.
   */
  public SimpleQueueSpecificationGenerator(ApplicationId appId) {
    this.appId = appId;
  }

  /**
   * Given a {@link FlowSpecification}.
   *
   * @param input {@link FlowSpecification}
   * @return A {@link Table}
   */
  @Override
  public Table<Node, String, Set<QueueSpecification>> create(FlowSpecification input) {
    Table<Node, String, Set<QueueSpecification>> table = HashBasedTable.create();

    String flow = input.getName();
    Map<String, FlowletDefinition> flowlets = input.getFlowlets();

    // Iterate through connections of a flow.
    for (FlowletConnection connection : input.getConnections()) {
      final String source = connection.getSourceName();
      final String target = connection.getTargetName();
      String sourceNamespace = connection.getSourceNamespace() == null ? appId.getNamespace() :
        connection.getSourceNamespace();
      Node sourceNode;

      Set<QueueSpecification> queueSpec;
      if (connection.getSourceType() == FlowletConnection.Type.FLOWLET) {
        sourceNode = new Node(connection.getSourceType(), source);
        queueSpec = generateQueueSpecification(appId, flow, connection,
                                               flowlets.get(target).getInputs(), flowlets.get(source).getOutputs());
      } else {
        sourceNode = new Node(connection.getSourceType(), sourceNamespace, source);
        queueSpec = generateQueueSpecification(appId, flow, connection, flowlets.get(target).getInputs(),
                                               ImmutableMap.<String, Set<Schema>>of(
                                                  connection.getSourceName(), ImmutableSet.of(STREAM_EVENT_SCHEMA)));
      }
      Set<QueueSpecification> queueSpecifications = table.get(sourceNode, target);
      if (queueSpecifications == null) {
        queueSpecifications = Sets.newHashSet();
        table.put(sourceNode, target, queueSpecifications);
      }
      queueSpecifications.addAll(queueSpec);
    }
    return table;
  }
}
