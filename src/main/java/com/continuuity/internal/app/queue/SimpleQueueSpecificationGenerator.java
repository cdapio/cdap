/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.io.Schema;
import com.continuuity.api.io.UnsupportedTypeException;
import com.continuuity.app.Id;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.internal.app.SchemaFinder;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Concrete implementation of {@link QueueSpecificationGenerator} for generating queue
 * names.
 */
public final class SimpleQueueSpecificationGenerator extends AbstractQueueSpecificationGenerator {
  /**
   * Account Name under which the stream names to generated.
   */
  private final Id.Account account;

  /**
   * Constructor that takes an account.
   *
   * @param account under which the stream is represented.
   */
  public SimpleQueueSpecificationGenerator(Id.Account account) {
    this.account = account;
  }

  /**
   * Given a {@link FlowSpecification}
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
    for(FlowletConnection connection : input.getConnections()) {
      final String source = connection.getSourceName();
      final String target = connection.getTargetName();
      final Node sourceNode = new Node(connection.getSourceType(), source);

      // If the source type is a flowlet, then we attempt to find a matching
      // connection that is equal or compatible. Equality has higher priority
      // over compatibility.
      if(connection.getSourceType() == FlowletConnection.Type.FLOWLET) {
        List<SchemaURIHolder> holders = findSchema(flow, flowlets.get(source), flowlets.get(target));
        for(SchemaURIHolder holder : holders) {
          if(table.contains(sourceNode, target)) {
            table.get(sourceNode, target).add(createSpec(holder.getOutput(), holder.getSchema()));
          } else {
            table.put(sourceNode, target, Sets.newHashSet(createSpec(holder.getOutput(), holder.getSchema())));
          }
        }
      }

      // Connection is a Stream.
      if(connection.getSourceType() == FlowletConnection.Type.STREAM) {
        try {
          // Create schema for StreamEvent and compare that with the inputs of the flowlet.
          final Schema schema = (new ReflectionSchemaGenerator()).generate((new TypeToken<StreamEvent>() {}).getType                                                                                                     ());
          Schema foundSchema = null;
          for(Map.Entry<String, Set<Schema>> entry : flowlets.get(target).getInputs().entrySet()) {
            foundSchema = SchemaFinder.findSchema(ImmutableSet.of(schema), entry.getValue());
            if(foundSchema != null) {
              break;
            }
          }

          if(foundSchema != null) {
            if(table.contains(sourceNode, target)) {
              table.get(sourceNode, target).add(createSpec(streamURI(account, source), foundSchema));
            } else {
              table.put(sourceNode, target,Sets.newHashSet(createSpec(streamURI(account, source), foundSchema)));
            }
          } else {
            throw new RuntimeException("Unable to find matching schema for connection between "
                                        + source + " and %s " + target);
          }
        } catch(UnsupportedTypeException e) {
          throw Throwables.propagate(e);
        }
      }
    }
    return table;
  }
}
