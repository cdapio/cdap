package CountRandom;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

/**
 * CountRandomDemo application contains a flow {@code CountRandom}.
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountRandomDemo")
      .addFlow(CountRandom.class)
      .create();
  }
}
