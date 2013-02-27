package com.continuuity.examples.counttokens;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * CountTokens application contains a flow {@code CountTokens} and is attached
 * to a stream named "text".  It utilizes a KeyValueTable to persist data.
 */
public class CountTokens implements Application {

  public static final String tableName = "tokenCountTable";

  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountTokensDemo")
      .setDescription("Example applicaiton that counts tokens")
      .withStreams()
        .add(new Stream("text"))
      .withDataSets()
        .add(new KeyValueTable(tableName))
      .withFlows()
        .add(new CountTokensFlow())
      .noProcedure()
      .build();
  }
}