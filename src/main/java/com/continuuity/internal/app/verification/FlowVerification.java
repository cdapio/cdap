package com.continuuity.internal.app.verification;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.app.Id;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.app.verification.AbstractVerifier;
import com.continuuity.app.verification.Verifier;
import com.continuuity.app.verification.VerifyResult;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.error.Err;
import com.continuuity.internal.app.SchemaFinder;
import com.continuuity.internal.app.queue.SimpleQueueSpecificationGenerator;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
* This verifies a give {@link com.continuuity.api.flow.Flow}
* <p/>
* <p>
* Following are the checks that are done for a {@link com.continuuity.api.flow.Flow}
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
public class FlowVerification extends AbstractVerifier implements Verifier<FlowSpecification> {

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
   * Verifies a single {@link FlowSpecification} for a {@link com.continuuity.api.flow.Flow}
   * defined within an {@link com.continuuity.api.Application}
   *
   * @param input to be verified
   * @return An instance of {@link VerifyResult} depending of status of verification.
   */
  @Override
  public VerifyResult verify(final FlowSpecification input) {
    String flowName = input.getName();

    // Checks if Flow name is an ID
    if(!isId(flowName)) {
      return VerifyResult.FAILURE(Err.NOT_AN_ID, "Flow");
    }

    // Check if there are no flowlets.
    if(input.getFlowlets().size() == 0) {
      return VerifyResult.FAILURE(Err.Flow.ATLEAST_ONE_FLOWLET, flowName);
    }

    // Check if there no connections.
    if(input.getConnections().size() == 0) {
      return VerifyResult.FAILURE(Err.Flow.ATLEAST_ONE_CONNECTION, flowName);
    }

    // We go through each Flowlet and verify the flowlets.
    for(Map.Entry<String, FlowletDefinition> entry : input.getFlowlets().entrySet()) {
      FlowletDefinition defn = entry.getValue();
      String flowletName = defn.getFlowletSpec().getName();

      // Check if the Flowlet Name is an ID.
      if(!isId(defn.getFlowletSpec().getName())) {
        return VerifyResult.FAILURE(Err.NOT_AN_ID, flowName + ":" + flowletName);
      }

      // We check if all the dataset names used are ids
      for(String dataSet : defn.getDatasets()) {
        if(!isId(dataSet)) {
          return VerifyResult.FAILURE(Err.NOT_AN_ID, flowName + ":" + flowletName + ":" + dataSet);
        }
      }
    }

    // FIXME: We should unify the logic here and the queue spec generation, as they are doing the same thing.
    Table<QueueSpecificationGenerator.Node, String, Set<QueueSpecification>> queueSpecTable
            = new SimpleQueueSpecificationGenerator(Id.Account.from("dummy")).create(input);

    // For all connections, there should be an entry in the table.
    for (FlowletConnection connection : input.getConnections()) {
      QueueSpecificationGenerator.Node node = new QueueSpecificationGenerator.Node(connection.getSourceType(),
                                                                                   connection.getSourceName());
      if (!queueSpecTable.contains(node, connection.getTargetName())) {
        return VerifyResult.FAILURE(Err.Flow.NO_INPUT_FOR_OUTPUT,
                                    connection.getTargetName(), connection.getSourceType(), connection.getSourceName());
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
        return VerifyResult.FAILURE(Err.Flow.MORE_OUTPUT_NOT_ALLOWED,
                                    node.getType(), node.getName(), outputs);
      }
    }

    return VerifyResult.SUCCESS();
  }

  private <K, V> Multimap<K, V> toMultimap(Map<K, ? extends Collection<V>> map) {
    Multimap<K, V> result = HashMultimap.create();

    for (Map.Entry<K, ? extends Collection<V>> entry : map.entrySet()) {
      result.putAll(entry.getKey(), entry.getValue());
    }
    return result;
  }
}
