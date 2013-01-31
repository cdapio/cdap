package CountOddAndEven;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;

/**
 * CountOddAndEvenDemo application contains a flow {@code CountOddAndEven} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountOddAndEvenDemo")
      .addFlow(CountOddAndEven.class)
      .create();
  }
}
