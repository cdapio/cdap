package CountOddAndEven;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

/**
 *
 */
public class Main implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .addFlow(CountOddAndEven.class)
      .create();
  }
}
