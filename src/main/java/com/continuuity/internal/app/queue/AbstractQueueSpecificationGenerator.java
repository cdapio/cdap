package com.continuuity.internal.app.queue;

import com.continuuity.api.flow.FlowletDefinition;
import com.continuuity.api.io.Schema;
import com.continuuity.app.program.Id;
import com.continuuity.app.queue.QueueName;
import com.continuuity.app.queue.QueueSpecification;
import com.continuuity.app.queue.QueueSpecificationGenerator;
import com.continuuity.internal.app.SchemaFinder;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * AbstractQueueSpecification builder for extracting commanality across
 * different implementation. We don't know how it would look for this yet :-)
 */
public abstract class AbstractQueueSpecificationGenerator implements QueueSpecificationGenerator {
  /**
   * Holds the matched schema and uri for a connection.
   */
  protected static class SchemaURIHolder {
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
   * Finds a equal or compatible schema connection between <code>source</code> and <code>target</code>
   * flowlet.
   */
  protected List<SchemaURIHolder> findSchema(String flow, FlowletDefinition source, FlowletDefinition target) {
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
   * @return An {@link java.net.URI} with schema as queue
   */
  protected URI queueURI(String flow, String flowlet, String output) {
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
  protected URI streamURI(Id.Account account, String stream) {
    try {
      URI uri = new URI("stream", Joiner.on("/").join(new Object[]{ "/", account.getId(), stream }), null);
      return uri;
    } catch(Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * @return An instance of {@link com.continuuity.app.queue.QueueSpecification} containing the URI for the queue
   * and the matching {@link com.continuuity.api.io.Schema}
   */
  protected QueueSpecification createSpec(final URI uri, final Schema schema) {
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
