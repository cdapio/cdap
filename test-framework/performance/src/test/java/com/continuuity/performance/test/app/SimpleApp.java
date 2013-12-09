/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.performance.test.app;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;
import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.OutputEmitter;
import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.metrics.Metrics;
import com.continuuity.data2.OperationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a sample Continuuity application  that is used for demonstration of performance testing.
 */
public class SimpleApp implements Application {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleApp.class);

  public static final String TABLE_NAME = "writeAndRead";

  /**
   * Configures the {@link com.continuuity.api.Application} by returning an
   * {@link com.continuuity.api.ApplicationSpecification}.
   *
   * @return An instance of {@code ApplicationSpecification}.
   */
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("SimpleApp")
      .setDescription("Flow that writes key=value then reads back the key")
      .withStreams()
      .add(new Stream("keyValues"))
      .withDataSets()
      .add(new KeyValueTable(TABLE_NAME))
      .withFlows()
      .add(new WriteAndReadFlow())
      .noProcedure()
      .noMapReduce()
      .noWorkflow()
      .build();
  }

  private static final class WriteAndReadFlow implements Flow {
    @Override
    public FlowSpecification configure() {
      return FlowSpecification.Builder.with()
        .setName("SimpleApp")
        .setDescription("Example flow that writes events from a stream to a data set and then reads them from there.")
        .withFlowlets()
        .add("source", new KeyValueSource())
        .add("writer", new WriterFlowlet())
        .add("reader", new ReaderFlowlet())
        .connect()
        .fromStream("keyValues").to("source")
        .from("source").to("writer")
        .from("writer").to("reader")
        .build();
    }

    // Flowlets will pass instances of a custom KeyAndValue class

    /**
     * Stores a string key and string value to pass between flowlets.
     */
    static class KeyAndValue {
      private String key;
      private String value;

      public KeyAndValue(String key, String value) {
        this.key = key;
        this.value = value;
      }

      public String getKey() {
        return this.key;
      }

      public String getValue() {
        return this.value;
      }

      @Override
      public String toString() {
        return key + "=" + value;
      }
    }
  }
  private static final class ReaderFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(ReaderFlowlet.class);
    private Metrics metrics;

    @UseDataSet(SimpleApp.TABLE_NAME)
    KeyValueTable kvTable;

    public void process(byte[] key) throws OperationException {
      LOG.debug(this.getContext().getName() + ": Received key " + Bytes.toString(key));

      // perform inline read of key and verify a value is found

      byte [] value = this.kvTable.read(key);
      metrics.count("stream.event", 1);

      if (value == null) {
        String msg = "No value found for key " + Bytes.toString(key);
        LOG.error(this.getContext().getName() + msg);
        throw new RuntimeException(msg);
      }

      LOG.debug(this.getContext().getName() + ": Read value (" + Bytes.toString(value) + ") for key ("
                  + Bytes.toString(key) + ")");
    }
  }

  private static final class WriterFlowlet extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(WriterFlowlet.class);
    private Metrics metrics;

    @UseDataSet(SimpleApp.TABLE_NAME)
    KeyValueTable kvTable;

    private OutputEmitter<byte[]> output;

    public void process(WriteAndReadFlow.KeyAndValue kv) throws OperationException {
      LOG.debug(this.getContext().getName() + ": Received KeyValue " + kv);

      this.kvTable.write(Bytes.toBytes(kv.getKey()), Bytes.toBytes(kv.getValue()));
      metrics.count("stream.event", 1);

      output.emit(Bytes.toBytes(kv.getKey()));
    }
  }

  private static final class KeyValueSource extends AbstractFlowlet {
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueSource.class);
    private Metrics metrics;

    private OutputEmitter<WriteAndReadFlow.KeyAndValue> output;

    public KeyValueSource() {
      super("source");
    }

    public void process(StreamEvent event) throws IllegalArgumentException {
      LOG.debug(this.getContext().getName() + ": Received event " + event);

      String text = Bytes.toString(Bytes.toBytes(event.getBody()));

      String [] fields = text.split("=");

      if (fields.length != 2) {
        throw new IllegalArgumentException("Input event must be in the form " + "'key=value', received '" + text + "'");
      }
      output.emit(new WriteAndReadFlow.KeyAndValue(fields[0], fields[1]));
      metrics.count("stream.event", 1);

    }
  }
}
