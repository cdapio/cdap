package com.continuuity.examples.wordcount;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.data.stream.Stream;

public class WordCount implements Application {
  public static void main(String[] args) {
    // Main method should be defined for Application to get deployed with Eclipse IDE plugin. DO NOT REMOVE IT
  }

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("WordCount")
      .setDescription("Example Word Count Application")
      .withStreams()
        .add(new Stream("wordStream"))
      .withDataSets()
        .add(new Table("wordStats"))
        .add(new KeyValueTable("wordCounts"))
        .add(new UniqueCountTable("uniqueCount"))
        .add(new AssociationTable("wordAssocs"))
      .withFlows()
        .add(new WordCounter())
      .withProcedures()
        .add(new RetrieveCounts())
      .build();
  }
}
