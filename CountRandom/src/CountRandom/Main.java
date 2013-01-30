package CountRandom;

import com.continuuity.api.data.dataset.table.Table;
import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

/**
 *
 */
public class Main implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .addFlow(CountRandom.class)
      .addDataSet(new Table("counters"))
      .create();
  }
}