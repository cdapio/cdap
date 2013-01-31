package CountCounts;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;
import com.continuuity.api.stream.Stream;

/**
 *
 */
public class Main implements Application {

  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountAppDemo")
      .addFlow(CountCounts.class)
      .addStream(new Stream("text"))
      .create();
  }
}
