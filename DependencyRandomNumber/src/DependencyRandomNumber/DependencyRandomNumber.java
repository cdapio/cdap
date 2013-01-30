package DependencyRandomNumber;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

/**
 * CountCounts class of the DependencyRandomNumber Flow.
 * 
 * Defines the meta-data, flowlets, and flow connections.
 */
public class DependencyRandomNumber implements Flow {
  public void configure(FlowSpecifier specifier) {
    /* Set the meta-data of the flow */
    specifier.name("DependencyRandomNumber");
    specifier.email("me@continuuity.com");
    specifier.application("End2End");
    
    /* Declare all the Flowlets within this Flow */
    specifier.flowlet("random-number-gen", RandomNumberSource.class, 1);
    specifier.flowlet("even-odd-counter", EvenOddCounter.class, 1);
    
    /* Declare the connections between the Flowlets in this Flow */
    specifier.connection("random-number-gen", "even-odd-counter");
  }
}
