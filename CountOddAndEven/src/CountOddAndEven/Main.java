package CountOddAndEven;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

/**
 * CountAppDemo application that contains multiple flows attached to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountAppDemo")
      .addFlow(CountOddAndEven.class)
      .create();
  }
}
