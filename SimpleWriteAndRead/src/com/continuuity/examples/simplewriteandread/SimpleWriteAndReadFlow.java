package com.continuuity.examples.simplewriteandread;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class SimpleWriteAndReadFlow implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("SimpleWriteAndRead")
      .setDescription("Example flow that writes then reads")
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
  public static class KeyAndValue {
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
