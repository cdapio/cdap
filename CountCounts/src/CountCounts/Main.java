package CountCounts;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;
import com.continuuity.api.stream.Stream;

/**
 *
 */
public class Main implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .addFlow(CountCounts.class)
      .addQuery(CountQuery.class)
      .addStream(new Stream("text"))
      .addDataSet(new CounterTable(Common.tableName))
      .create();
  }
}
