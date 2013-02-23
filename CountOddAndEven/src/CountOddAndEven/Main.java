package CountOddAndEven;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;

/**
 * CountOddAndEvenDemo application contains a flow {@code CountOddAndEven} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountOddAndEvenDemo")
      .setDescription("")
      .noStream()
      .noDataSet()
      .withFlows()
        .add(new CountOddAndEven())
      .noProcedure()
      .build();
  }
}
