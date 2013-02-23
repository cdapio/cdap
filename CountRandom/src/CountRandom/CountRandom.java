package CountRandom;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountRandom implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountRandom")
      .setDescription("CountRandom Flow")
      .withFlowlets()
        .add("source", new RandomSource())
        .add("splitter", new NumberSplitter())
        .add("counter", new NumberCounter())
      .connect()
        .from("source").to("splitter")
        .from("splitter").to("counter")
      .build();
  }
}
