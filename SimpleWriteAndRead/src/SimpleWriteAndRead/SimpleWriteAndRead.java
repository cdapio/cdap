package SimpleWriteAndRead;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class SimpleWriteAndRead implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("SimpleWriteAndRead")
      .setDescription("")
      .withFlowlets()
        .add("source", new StreamSource())
        .add("writer", new WriterFlowlet())
        .add("reader", new ReaderFlowlet())
      .connect()
        .from("source").to("writer")
        .from("writer").to("reader")
      .build();
  }
}
