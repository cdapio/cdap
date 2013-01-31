package DependencyRandomNumber;

import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

public class Main implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("DependencyRandomNumberDemo")
      .addDataSet(new KeyValueTable(Common.tableName))
      .addFlow(DependencyRandomNumber.class)
      .create();
  }
}