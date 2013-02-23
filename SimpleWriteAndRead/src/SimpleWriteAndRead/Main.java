package SimpleWriteAndRead;

import com.continuuity.api.Application;
import com.continuuity.api.ApplicationSpecification;
import com.continuuity.api.data.dataset.KeyValueTable;
import com.continuuity.api.data.stream.Stream;

/**
 * DataFabricDemo application is a application with a single flow that demonstrates
 * how to read and write from data fabric. It's attached to a single stream named "text".
 */
public class Main implements Application {
  @Override
  public ApplicationSpecification configure() {
    return ApplicationSpecification.Builder.with()
      .setName("SimpleWriteAndReadDemo")
      .setDescription("")
      .withStreams().add(new Stream("text"))
      .withDataSets().add(new KeyValueTable(Common.tableName))
      .withFlows().add(new SimpleWriteAndRead())
      .noProcedure()
      .build();
  }
//  @Override
//  public ApplicationSpecification configure() {
//    return ApplicationSpecification.builder()
//      .setApplicationName("SimpleWriteAndReadDemo")
//      .addFlow(SimpleWriteAndRead.class)
//      .addStream(new Stream("text"))
//      .addDataSet(new KeyValueTable(Common.tableName))
//      .create();
//  }
}