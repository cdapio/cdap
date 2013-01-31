package CountCounts;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;
import com.continuuity.api.stream.Stream;

/**
 * CountCountsDemo application contains a flow {@code CountCounts} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountCountsDemo")
      .addFlow(CountCounts.class)
      .addStream(new Stream("text"))
      .create();
  }
}
