package CountOddAndEven;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class CountOddAndEven implements Flow {
  public void configure(FlowSpecifier specifier) {
    specifier.name("CountOddAndEven");
    specifier.email("me@continuuity.com");
    specifier.application("Examples");

    // Configure all the flowlets.
    specifier.flowlet("NumGenerator", RandomNumberGenerator.class);
    specifier.flowlet("OddOrEven", OddOrEven.class);
    specifier.flowlet("EvenCounter", EvenCounter.class);
    specifier.flowlet("OddCounter", OddCounter.class);

    // Configure all the edges.
    specifier.connection("NumGenerator", "OddOrEven");
    specifier.connection("OddOrEven", "odd", "OddCounter", "in");
    specifier.connection("OddOrEven", "even", "EvenCounter", "in");
  }
}
