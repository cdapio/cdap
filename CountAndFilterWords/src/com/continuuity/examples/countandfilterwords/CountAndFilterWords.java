package com.continuuity.examples.countandfilterwords;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * CountAndFilterWordsDemo application contains a flow {@code CountAndFilterWords} and is attached
 * to a stream named "text"
 */
public class CountAndFilterWords implements Application {

  public static final String tableName = "filterTable";

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountAndFilterWords")
      .setDescription("Example word filter and count application")
      .withStreams()
        .add(new Stream("text"))
      .withDataSets()
        .add(new KeyValueTable(tableName))
      .withFlows()
        .add(new CountAndFilterWordsFlow())
      .noProcedure()
      .build();
  }
}
