package CountAndFilterWords;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * CountAndFilterWordsDemo application contains a flow {@code CountAndFilterWords} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountAndFilterWordsDemo")
      .setDescription("")
      .withStreams()
        .add(new Stream("text"))
      .withDataSets()
        .add(new KeyValueTable(Common.counterTableName))
      .withFlows()
        .add(new CountAndFilterWords())
      .noProcedure()
      .build();
  }
}
