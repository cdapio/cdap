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
//  public void configure(FlowSpecifier specifier) {
//    specifier.name("CountRandom");
//    specifier.email("me@continuuity.com");
//    specifier.dataset("counters");
//    specifier.application("CountRandom");
//    specifier.flowlet("gen", RandomSource.class, 1);
//    specifier.flowlet("split", NumberSplitter.class, 1);
//    specifier.flowlet("count", NumberCounter.class, 1);
//    specifier.connection("gen", "split");
//    specifier.connection("split", "count");
//  }

}
