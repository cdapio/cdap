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
import com.continuuity.app.program.Id;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.internal.app.SchemaFinder;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;

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
  private final Id.Account account;

  /**
   * Holds the matched schema and uri for a connection.
   */
  class SchemaURIHolder {
    private Schema schema;
    private URI output;

    SchemaURIHolder(final Schema schema, final URI output) {
      this.schema = schema;
      this.output = output;
    }

    Schema getSchema() {
      return schema;
    }

    URI getOutput() {
      return output;
    }
  }

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
  public Table<String, String, Set<QueueSpecification>> create(FlowSpecification input) {
    String flow = input.getName();
    Table<String, String, Set<QueueSpecification>> table = HashBasedTable.create();
    Map<String, FlowletDefinition> flowlets = input.getFlowlets();

    // Iterate through connections of a flow.
    for(FlowletConnection connection : input.getConnections()) {
      final String source = connection.getSourceName();
      final String target = connection.getTargetName();

      // If the source type is a flowlet, then we attempt to find a matching
      // connection that is equal or compatible. Equality has higher priority
      // over compatibility.
      if(connection.getSourceType() == FlowletConnection.SourceType.FLOWLET) {
        List<SchemaURIHolder> holders = findSchema(flow, flowlets.get(source), flowlets.get(target));
        for(SchemaURIHolder holder : holders) {
          if(table.contains(source, target)) {
            table.get(source, target).add(createSpec(holder.getOutput(), holder.getSchema()));
          } else {
            table.put(source, target, Sets.newHashSet(createSpec(holder.getOutput(), holder.getSchema())));
          }
        }
      }

      // Connection is a Stream.
      if(connection.getSourceType() == FlowletConnection.SourceType.STREAM) {
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
            if(table.contains(source, target)) {
              table.get(source, target).add(createSpec(streamURI(account, source), foundSchema));
            } else {
              table.put(source, target,Sets.newHashSet(createSpec(streamURI(account, source), foundSchema)));
            }
          } else {
            throw new RuntimeException("Unable to find matching schema for connection between " + source + " and %s " +
                                        target);
          }
        } catch(UnsupportedTypeException e) {
          throw Throwables.propagate(e);
        }
      }

    }
    return table;
  }

  /**
   * Finds a equal or compatible schema connection between <code>source</code> and <code>target</code>
   * flowlet.
   */
  private List<SchemaURIHolder> findSchema(String flow, FlowletDefinition source, FlowletDefinition target) {
    ImmutableList.Builder<SchemaURIHolder> HOLDER = ImmutableList.builder();
    Map<String, Set<Schema>> output = source.getOutputs();
    Map<String, Set<Schema>> input = target.getInputs();

    Schema foundSchema = null;
    String foundOutputName = "";

    // Iterate through all the outputs and for each output we go over
    // all the inputs. If there is exact match of schema then we pick
    // that over the compatible one.
    for(Map.Entry<String, Set<Schema>> entryOutput : output.entrySet()) {
      String outputName = entryOutput.getKey();
      for(Map.Entry<String, Set<Schema>> entryInput : input.entrySet()) {
        String inputName = entryInput.getKey();

        // When the output name is same as input name - we check if their schema's
        // are same (equal or compatible)
        if(outputName.equals(inputName)) {
          Schema s = SchemaFinder.findSchema(entryOutput.getValue(), entryInput.getValue());
          HOLDER.add(new SchemaURIHolder(s, queueURI(flow, source.getFlowletSpec().getName(), outputName)));
        }

        // If not found there, we do a small optimization where we check directly if
        // the output matches the schema of ANY_INPUT schema. If it doesn't then we
        // have an issue else we are good.
        if(input.containsKey(FlowletDefinition.ANY_INPUT)) {
          foundSchema = SchemaFinder.findSchema(entryOutput.getValue(), input.get(FlowletDefinition.ANY_INPUT));
          foundOutputName = outputName;
        }
      }
    }

    if(foundSchema != null){
      HOLDER.add(new SchemaURIHolder(foundSchema, queueURI(flow, source.getFlowletSpec().getName(), foundOutputName)));
    }
    return HOLDER.build();
  }

  /**
   * Generates a Queue URI for connectivity between two flowlets.
   *
   * @param flow    Name of the Flow for which this queue is being generated
   * @param flowlet Of the queue is connected to
   * @param output  name of the queue.
   * @return An {@link URI} with schema as queue
   */
  private URI queueURI(String flow, String flowlet, String output) {
    try {
      URI uri = new URI("queue", Joiner.on("/").join(new Object[]{ "/", flow, flowlet, output }), null);
      return uri;
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Generates an URI for the stream.
   *
   * @param account The stream belongs to
   * @param stream  connected to flow
   * @return An {@link URI} with schema as stream
   */
  private URI streamURI(Id.Account account, String stream) {
    try {
      URI uri = new URI("stream", Joiner.on("/").join(new Object[]{ "/", account.getId(), stream }), null);
      return uri;
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return An instance of {@link QueueSpecification} containing the URI for the queue
   * and the matching {@link Schema}
   */
  private QueueSpecification createSpec(final URI uri, final Schema schema) {
    return new QueueSpecification() {
      @Override
      public QueueName getQueueName() {
        return QueueName.from(uri);
      }

      @Override
      public Schema getSchemas() {
        return schema;
      }
    };
  }
}
