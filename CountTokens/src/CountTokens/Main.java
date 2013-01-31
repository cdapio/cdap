package CountTokens;

import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;
import com.continuuity.api.stream.Stream;

/**
 * CountTokensDemo application contains a flow {@code CountTokens} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountTokensDemo")
      .addFlow(CountTokens.class)
      .addStream(new Stream("text"))
      .create();
  }
}