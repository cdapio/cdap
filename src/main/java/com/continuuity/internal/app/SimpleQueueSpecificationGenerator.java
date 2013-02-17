/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.internal.app;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.io.Schema;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Concrete implementation of {@link QueueSpecificationGenerator} for generating queue
 * names.
 */
public final class SimpleQueueSpecificationGenerator implements QueueSpecificationGenerator {
  /**
   * Account Name under which the stream names to generated.
   */
  private final String account;

  /**
   * Constructor that takes an account.
   *
   * @param account under which the stream is represented.
   */
  public SimpleQueueSpecificationGenerator(String account) {
    this.account = account;
  }

  /**
   * This class represents a node in the DAG.
   */
  private static final class Node {
    private final FlowletConnection.SourceType type;
    private final String source;

    public Node(FlowletConnection.SourceType type, String source) {
      this.type = type;
      this.source = source;
    }

    public FlowletConnection.SourceType getSourceType() {
      return type;
    }

    public String getSourceName() {
      return source;
    }
  }

  @Override
  public Table<String, String, QueueSpecification> create(FlowSpecification specification) {
    Table<String, String, QueueSpecification> table = HashBasedTable.create();
    final String flow = specification.getName();
    Map<String, FlowletDefinition> flowlets = specification.getFlowlets();

    // Create Adjacency List first to simplfy further processing.
    Map<String, List<Node>> adjacencyList = Maps.newHashMap();
    for(FlowletConnection connection : specification.getConnections()) {
      if(adjacencyList.containsKey(connection.getTargetName())) {
        adjacencyList.get(connection.getTargetName())
          .add(new Node(connection.getSourceType(), connection.getSourceName()));
      } else {
        List<Node> a = Lists.newArrayList();
        a.add(new Node(connection.getSourceType(), connection.getSourceName()));
        adjacencyList.put(connection.getTargetName(), a);
      }
    }

    // Iterate through adjacency list.
    for(Map.Entry<String, List<Node>> entry : adjacencyList.entrySet()) {
      String targetNodeName = entry.getKey();
      final Map<String, Set<Schema>> inputs = flowlets.get(targetNodeName).getInputs();

      // Within in each node, go through all the nodes the targetNodeName
      // is connected to. If source node type is stream, then generate
      // stream URI, if for flowlet, then check if there is input available
      // to process the output generated. If there is not, check if there
      // ANY available. If it's available, then please make sure the schema's
      // are equal. Not sure, if we have to check for compatibility.
      for(Node source : entry.getValue()) {
        final String sourceNodeName = source.getSourceName();
        FlowletConnection.SourceType sourceNodeType = source.getSourceType();

        if(sourceNodeType == FlowletConnection.SourceType.STREAM) {
          final Map<String, Set<Schema>> outputs = flowlets.get(targetNodeName).getInputs();
          QueueSpecification s = new QueueSpecification() {
            @Override
            public QueueName getQueueName() {
              return QueueName.from(generateStreamURI(account, sourceNodeName));
            }

            @Override
            public Set<Schema> getSchemas() {
              return outputs.get(FlowletDefinition.ANY_INPUT);
            }
          };
          table.put(sourceNodeName, targetNodeName, s);
        }

        if(sourceNodeType == FlowletConnection.SourceType.FLOWLET) {
          final Map<String, Set<Schema>> outputs = flowlets.get(sourceNodeName).getOutputs();
          for(final Map.Entry<String, Set<Schema>> output : outputs.entrySet()) {
            if(inputs.containsKey(output.getKey())) {
              QueueSpecification s = new QueueSpecification() {
                @Override
                public QueueName getQueueName() {
                  return QueueName.from(generateQueueURI(flow, sourceNodeName, output.getKey()));
                }

                @Override
                public Set<Schema> getSchemas() {
                  return output.getValue();
                }
              };
              table.put(sourceNodeName, targetNodeName, s);
            } else {
              if(inputs.containsKey(FlowletDefinition.ANY_INPUT)) {
                if(compareSchema(inputs.get(FlowletDefinition.ANY_INPUT), outputs.get("out"))) {
                  QueueSpecification s = new QueueSpecification() {
                    @Override
                    public QueueName getQueueName() {
                      return QueueName.from(generateQueueURI(flow, sourceNodeName, "out"));
                    }

                    @Override
                    public Set<Schema> getSchemas() {
                      return outputs.get(FlowletDefinition.ANY_INPUT);
                    }
                  };
                  table.put(sourceNodeName, targetNodeName, s);
                }
              }
            }
          }
        }
      }
    }
    return table;
  }

  /**
   * Generates a Queue URI for connectivity between two flowlets.
   *
   * @param flow    Name of the Flow for which this queue is being generated
   * @param flowlet Of the queue is connected to
   * @param output  name of the queue.
   * @return An {@link URI} with schema as queue
   */
  private URI generateQueueURI(String flow, String flowlet, String output) {
    try {
      URI uri = new URI("queue", Joiner.on("/").join(new Object[]{ "/", flow, flowlet, output }), null);
      return uri;
    } catch(Exception e) {
      Throwables.propagate(e);
      // Unreachable.
      return null;
    }
  }

  /**
   * Generates an URI for the queue.
   *
   * @param account The stream belongs to
   * @param stream  connected to flow
   * @return An {@link URI} with schema as stream
   */
  private URI generateStreamURI(String account, String stream) {
    try {
      URI uri = new URI("stream", Joiner.on("/").join(new Object[]{ "/", account, stream }), null);
      return uri;
    } catch(Exception e) {
      Throwables.propagate(e);
      // Unreachable.
      return null;
    }
  }

  /**
   * Checks complete schema for equality.
   *
   * @param output Schema
   * @param input  Schema
   * @return true if they same; false otherwise.
   */
  private boolean compareSchema(Set<Schema> output, Set<Schema> input) {
    for(Schema outputSchema : output) {
      int equal = 0;
      int compatible = 0;
      for(Schema inputSchema : input) {
        if(!outputSchema.equals(inputSchema)) {
          return false;
        }
      }
    }
    return true;
  }
}
