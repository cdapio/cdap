/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.app.Id;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.internal.io.Schema;
import com.continuuity.internal.io.UnsupportedTypeException;
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
  private final Id.Application appId;

  /**
   * Constructor that takes an appId.
   *
   * @param appId under which the stream is represented.
   */
  public SimpleQueueSpecificationGenerator(Id.Application appId) {
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
      final Node sourceNode = new Node(connection.getSourceType(), source);

      Set<QueueSpecification> queueSpec;
      if (connection.getSourceType() == FlowletConnection.Type.FLOWLET) {
        queueSpec = generateQueueSpecification(appId, flow, connection,
                                               flowlets.get(target).getInputs(), flowlets.get(source).getOutputs());
      } else {
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
