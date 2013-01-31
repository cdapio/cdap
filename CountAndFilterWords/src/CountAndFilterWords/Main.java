package CountAndFilterWords;

import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.flow.Application;
import com.continuuity.api.flow.ApplicationSpecification;
import com.continuuity.api.stream.Stream;

/**
 * CountAndFilterWordsDemo application contains a flow {@code CountAndFilterWords} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.builder()
      .setApplicationName("CountAndFilterWordsDemo")
      .addFlow(CountAndFilterWords.class)
      .addStream(new Stream("text"))
      .addDataSet(new KeyValueTable(Common.counterTableName))
      .create();
  }
}
