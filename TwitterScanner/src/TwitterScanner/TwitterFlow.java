/*
 * twitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package TwitterScanner;

import com.continuuity.api.flow.Flow;
import com.continuuity.api.flow.FlowSpecifier;

public class TwitterFlow implements Flow {

  @Override
  public void configure(FlowSpecifier flowSpecifier) {

    // Set up some meta data
    flowSpecifier.name("TwitterScanner");
    flowSpecifier.email("todd@continuuity.com");
    flowSpecifier.application("Flow Examples");

    // Now wire up the Flow for real
    flowSpecifier.flowlet("Fetch", TwitterGenerator.class, 1);
    flowSpecifier.flowlet("Process", TwitterProcessor.class, 1);

    // Connect to the next Flowlet
    flowSpecifier.connection("Fetch", "Process");


  }
}
