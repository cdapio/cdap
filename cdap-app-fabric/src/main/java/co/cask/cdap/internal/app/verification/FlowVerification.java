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

package co.cask.cdap.internal.app.verification;

import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.app.queue.QueueSpecification;
import co.cask.cdap.app.queue.QueueSpecificationGenerator;
import co.cask.cdap.app.verification.VerifyResult;
import co.cask.cdap.error.Err;
import co.cask.cdap.internal.app.queue.SimpleQueueSpecificationGenerator;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.proto.Id;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
* This verifies a give {@link co.cask.cdap.api.flow.Flow}.
* <p/>
* <p>
* Following are the checks that are done for a {@link co.cask.cdap.api.flow.Flow}
* <ul>
* <li>Verify Flow Meta Information - Name is id</li>
* <li>There should be atleast one or two flowlets</li>
* <li>Verify information for each Flowlet</li>
* <li>There should be atleast one connection</li>
* <li>Verify schema's across connections on flowlet</li>
* </ul>
* <p/>
* </p>
*/
public class FlowVerification extends ProgramVerification<FlowSpecification> {

  /**
   * Verifies a single {@link FlowSpecification} for a {@link co.cask.cdap.api.flow.Flow}.
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(Id.Application appId, final FlowSpecification input) {
    VerifyResult verifyResult = super.verify(appId, input);
    if (!verifyResult.isSuccess()) {
      return verifyResult;
    }

    String flowName = input.getName();

    // Check if there are no flowlets.
    if (input.getFlowlets().isEmpty()) {
      return VerifyResult.failure(Err.Flow.ATLEAST_ONE_FLOWLET, flowName);
    }

    // Check if there no connections.
    if (input.getConnections().isEmpty()) {
      return VerifyResult.failure(Err.Flow.ATLEAST_ONE_CONNECTION, flowName);
    }

    // We go through each Flowlet and verify the flowlets.

    // First collect all source flowlet names
    Set<String> sourceFlowletNames = Sets.newHashSet();
    for (FlowletConnection connection : input.getConnections()) {
      if (connection.getSourceType() == FlowletConnection.Type.FLOWLET) {
        sourceFlowletNames.add(connection.getSourceName());
      }
    }

    for (Map.Entry<String, FlowletDefinition> entry : input.getFlowlets().entrySet()) {
      FlowletDefinition defn = entry.getValue();
      String flowletName = defn.getFlowletSpec().getName();

      // Check if the Flowlet Name is an ID.
      if (!isId(defn.getFlowletSpec().getName())) {
        return VerifyResult.failure(Err.NOT_AN_ID, flowName + ":" + flowletName);
      }

      // We check if all the dataset names used are ids
      for (String dataSet : defn.getDatasets()) {
        if (!isId(dataSet)) {
          return VerifyResult.failure(Err.NOT_AN_ID, flowName + ":" + flowletName + ":" + dataSet);
        }
      }

      // Check if the flowlet has output, it must be appear as source flowlet in at least one connection
      if (entry.getValue().getOutputs().size() > 0 && !sourceFlowletNames.contains(flowletName)) {
        return VerifyResult.failure(Err.Flow.OUTPUT_NOT_CONNECTED, flowName, flowletName);
      }
    }

    // NOTE: We should unify the logic here and the queue spec generation, as they are doing the same thing.
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecTable
            = new SimpleQueueSpecificationGenerator(appId).create(input);

    // For all connections, there should be an entry in the table.
    for (FlowletConnection connection : input.getConnections()) {
      QueueSpecificationGenerator.Node node = new QueueSpecificationGenerator.Node(connection.getSourceType(),
                                                                                   connection.getSourceName());
      if (!queueSpecTable.contains(node, connection.getTargetName())) {
        return VerifyResult.failure(Err.Flow.NO_INPUT_FOR_OUTPUT,
                                    flowName, connection.getTargetName(),
                                    connection.getSourceType(), connection.getSourceName());
      }
    }

    // For each output entity, check for any unconnected output
    for (QueueSpecificationGenerator.Node node : queueSpecTable.rowKeySet()) {
      // For stream output, no need to check
      if (node.getType() == FlowletConnection.Type.STREAM) {
        continue;
      }

      // For all outputs of a flowlet, remove all the matched connected schema, if there is anything left,
      // then it's a incomplete flow connection (has output not connect to any input).
      Multimap<String, Schema> outputs = toMultimap(input.getFlowlets().get(node.getName()).getOutputs());
      for (Map.Entry<String, Set<QueueSpecification>> entry : queueSpecTable.row(node).entrySet()) {
        for (QueueSpecification queueSpec : entry.getValue()) {
          outputs.remove(queueSpec.getQueueName().getSimpleName(), queueSpec.getOutputSchema());
        }
      }

      if (!outputs.isEmpty()) {
        return VerifyResult.failure(Err.Flow.MORE_OUTPUT_NOT_ALLOWED,
                                    flowName, node.getType().toString().toLowerCase(), node.getName(), outputs);
      }
    }

    return VerifyResult.success();
  }

  @Override
  protected String getName(FlowSpecification input) {
    return input.getName();
  }

  private <K, V> Multimap<K, V> toMultimap(Map<K, ? extends Collection<V>> map) {
    Multimap<K, V> result = HashMultimap.create();

    for (Map.Entry<K, ? extends Collection<V>> entry : map.entrySet()) {
      result.putAll(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
