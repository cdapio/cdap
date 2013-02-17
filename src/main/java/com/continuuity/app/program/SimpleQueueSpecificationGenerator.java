/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.program;

import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.FlowletConnection;
import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.io.Schema;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class SimpleQueueSpecificationGenerator implements QueueSpecificationGenerator {

  @Override
  public Map<String, QueueSpecificationHolder> create(String account, String app, FlowSpecification specification) {
    Map<String, QueueSpecificationHolder> mappings = Maps.newTreeMap();
    String flow = specification.getName();

    List<FlowletConnection> connections = specification.getConnections();
    Map<String, FlowletDefinition> flowlets = specification.getFlowlets();

    for(FlowletConnection connection : connections) {

      if(connection.getSourceType() == FlowletConnection.SourceType.STREAM) {
        FlowletDefinition inputTo = flowlets.get(connection.getTargetName());
        final URI inputURI = generateStreamURI(account, connection.getSourceName());
        Set<Schema> inSchema = flowlets.get(connection.getTargetName()).getInputs().get(FlowletDefinition.ANY_INPUT);

        Map<String, Set<QueueSpecification>> inputs = Maps.newTreeMap();
        Set<QueueSpecification> inputQueueSpecs = Sets.newHashSet();
        for(final Schema schema : inSchema) {
          QueueSpecification spec = new QueueSpecification() {
            @Override
            public URI getURI() {
              return inputURI;
            }

            @Override
            public Schema getSchema() {
              return schema;
            }
          };
          inputQueueSpecs.add(spec);
        }
        inputs.put(FlowletDefinition.ANY_INPUT, inputQueueSpecs);
        Map<String, Set<QueueSpecification>> outputs = Maps.newTreeMap();
        for(Map.Entry<String, Set<Schema>> entry : flowlets.get(connection.getTargetName()).getOutputs().entrySet()) {
          final URI outputURI = generateQueueURI("queue", flow, connection.getTargetName(), entry.getKey());
          Set<QueueSpecification> outputQueueSpecs = Sets.newHashSet();
          for(final Schema schema : entry.getValue()) {
            QueueSpecification spec = new QueueSpecification() {
              @Override
              public URI getURI() {
                return outputURI;
              }

              @Override
              public Schema getSchema() {
                return schema;
              }
            };
            outputQueueSpecs.add(spec);
          }
          outputs.put(entry.getKey(), outputQueueSpecs);
        }
        mappings.put(connection.getTargetName(), new QueueSpecificationHolder(inputs, outputs));
      } else if(connection.getSourceType() == FlowletConnection.SourceType.FLOWLET) {
        Map<String, Set<QueueSpecification>> inputs = Maps.newTreeMap();
        Map<String, Set<QueueSpecification>> outputs = Maps.newTreeMap();
        Set<QueueSpecification> inputQueueSpecs = Sets.newHashSet();

        for(Map.Entry<String, Set<Schema>> entry : flowlets.get(connection.getTargetName()).getOutputs().entrySet()) {
          String q = entry.getKey();
          if(entry.getKey().equals(FlowletDefinition.ANY_INPUT)) {
            q = "out";
          }
          final URI outputURI = generateQueueURI("queue", flow, connection.getSourceName(), q);
          Set<QueueSpecification> outputQueueSpecs = Sets.newHashSet();
          for(final Schema schema : entry.getValue()) {
            QueueSpecification spec = new QueueSpecification() {
              @Override
              public URI getURI() {
                return outputURI;
              }

              @Override
              public Schema getSchema() {
                return schema;
              }
            };
            outputQueueSpecs.add(spec);
          }
          outputs.put(entry.getKey(), outputQueueSpecs);
        }

        for(Map.Entry<String, Set<Schema>> entry : flowlets.get(connection.getTargetName()).getInputs().entrySet()) {
          String q = entry.getKey();
          if(entry.getKey().equals(FlowletDefinition.ANY_INPUT)) {
            q = "out";
          }
          final URI outputURI = generateQueueURI("queue", flow, connection.getTargetName(), q);
          for(final Schema schema : entry.getValue()) {
            QueueSpecification spec = new QueueSpecification() {
              @Override
              public URI getURI() {
                return outputURI;
              }

              @Override
              public Schema getSchema() {
                return schema;
              }
            };
            inputQueueSpecs.add(spec);
          }
          inputs.put(entry.getKey(), inputQueueSpecs);
        }
        mappings.put(connection.getTargetName(), new QueueSpecificationHolder(inputs, outputs));
      }
    }
    return mappings;
  }

  private URI generateQueueURI(String type, String flow, String flowlet, String stream){
    try {
      URI uri = new URI(type, Joiner.on("/").join(new Object[] { "/", flow, flowlet, stream }), null);
      return uri;
    } catch (Exception e) {
      Throwables.propagate(e);
      // Unreachable.
      return null;
    }
  }

  private URI generateStreamURI(String account, String stream){
    try {
      URI uri = new URI("stream", Joiner.on("/").join(new Object[] { "/", account, stream }), null);
      return uri;
    } catch (Exception e) {
      Throwables.propagate(e);
      // Unreachable.
      return null;
    }
  }
}
