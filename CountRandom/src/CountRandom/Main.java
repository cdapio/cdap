package CountRandom;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.table.Table;

/**
 * CountRandomDemo application contains a flow {@code CountRandom}.
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountRandomDemo")
      .setDescription("")
      .noStream()
      .withDataSets().add(new Table("counters"))
      .withFlows().add(new CountRandom())
      .noProcedure()
      .build();
  }
//  @Override
//  public ApplicationSpecification configure() {
//    return ApplicationSpecification.builder()
//      .setApplicationName("CountRandomDemo")
//      .addFlow(CountRandom.class)
//      .addDataSet(new Table("counters"))
//      .create();
//  }
}
