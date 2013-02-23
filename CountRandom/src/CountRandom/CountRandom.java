package CountRandom;


import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountRandom implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountRandom")
      .setDescription("")
      .withFlowlets()
        .add("gen", new RandomSource())
        .add("split", new NumberSplitter())
        .add("count", new NumberCounter())
      .connect()
        .from("gen").to("split")
        .from("split").to("count")
      .build();
  }
}
