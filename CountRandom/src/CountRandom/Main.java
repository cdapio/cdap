package CountRandom;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

/**
 *
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountAppDemo")
      .addFlow(CountRandom.class)
      .create();
  }
}
