package CountTokens;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * CountTokensDemo application contains a flow {@code CountTokens} and is attached
 * to a stream named "text"
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("CountTokensDemo")
      .setDescription("")
      .withStreams().add(new Stream("text"))
      .withDataSets().add(new KeyValueTable(Common.tableName))
      .withFlows().add(new CountTokens())
      .noProcedure()
      .build();
  }
//  @Override
//  public ApplicationSpecification configure() {
//    return ApplicationSpecification.builder()
//      .setApplicationName("CountTokensDemo")
//      .addFlow(CountTokens.class)
//      .addStream(new Stream("text"))
//      .addDataSet(new KeyValueTable(Common.tableName))
//      .create();
//  }
}