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
      .setName("CountRandomApp")
      .setDescription("Count Random Application")
      .noStream()
      .withDataSets().add(new Table("counters"))
      .withFlows().add(new CountRandom())
      .noProcedure()
      .build();
  }
}
