package com.continuuity.examples.wordcount;

import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;
import com.continuuity.api.stream.Stream;

public class WordCountApp implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
        .addStream(new Stream("wordStream"))
        .addDataSet(new Table("wordStats"))
        .addDataSet(new Table("wordCounts"))
        .addDataSet(new UniqueCountTable("uniqueCount"))
        .addDataSet(new WordAssocTable("wordAssocs"))
        .addFlow(WordCountFlow.class)
        .addQuery(WordCountQuery.class)
        .create();
  }
}
