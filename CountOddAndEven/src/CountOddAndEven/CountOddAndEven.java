package CountOddAndEven;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecification;

public class CountOddAndEven implements Flow {
  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName("CountOddAndEven")
      .setDescription("Flow for counting words")
      .withFlowlets()
        .add("NumGenerator", new RandomNumberGenerator())
        .add("OddOrEven", new OddOrEven())
        .add("EvenCounter", new EvenCounter())
        .add("OddCounter", new OddCounter())
      .connect()
        .from("NumGenerator").to("OddOrEven")
        .from("OddOrEven").to("EvenCounter")
        .from("OddOrEven").to("OddCounter")
      .build();
  }
//  public void configure(FlowSpecifier specifier) {
//    specifier.name("CountOddAndEven");
//    specifier.email("me@continuuity.com");
//    specifier.application("Examples");
//
//    // Configure all the flowlets.
//    specifier.flowlet("NumGenerator", RandomNumberGenerator.class);
//    specifier.flowlet("OddOrEven", OddOrEven.class);
//    specifier.flowlet("EvenCounter", EvenCounter.class);
//    specifier.flowlet("OddCounter", OddCounter.class);
//
//    // Configure all the edges.
//    specifier.connection("NumGenerator", "OddOrEven");
//    specifier.connection("OddOrEven", "out", "OddCounter", "in");
//    specifier.connection("OddOrEven", "even", "EvenCounter", "in");
//  }
}
