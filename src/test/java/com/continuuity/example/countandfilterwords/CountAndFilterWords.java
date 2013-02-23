package com.continuuity.example.countandfilterwords;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * CountAndFilterWordsDemo application contains a flow {@code CountAndFilterFlow} and is attached
 * to a stream named "text"
 */
public class CountAndFilterWords implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountAndFilterWords")
      .setDescription("")
      .withStreams()
        .add(new Stream("text"))
      .withDataSets()
        .add(new KeyValueTable(Common.counterTableName))
      .withFlows()
        .add(new CountAndFilterFlow())
      .noProcedure()
      .build();
  }
}
