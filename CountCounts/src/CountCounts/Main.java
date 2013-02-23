package CountCounts;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * CountCountsDemo application contains a flow {@code CountCounts} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountCountsDemo")
      .setDescription("Application for counting counts of words")
      .withStreams().add(new Stream("text"))
      .withDataSets().add(new KeyValueTable(Common.tableName))
      .withFlows().add(new CountCounts())
      .withProcedures().add(new CountQuery())
      .build();
  }
}