package com.continuuity.examples.simplewriteandread;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * DataFabricDemo application is a application with a single flow that demonstrates
 * how to read and write from data fabric. It's attached to a single stream named "text".
 */
public class SimpleWriteAndRead implements Application {

  public static final String tableName = "writeAndRead";

  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("SimpleWriteAndRead")
      .setDescription("Flow that writes key=value then reads back the key")
      .withStreams()
        .add(new Stream("keyValues"))
      .withDataSets()
        .add(new KeyValueTable(tableName))
      .withFlows()
        .add(new SimpleWriteAndReadFlow())
      .noProcedure()
      .build();
  }
}