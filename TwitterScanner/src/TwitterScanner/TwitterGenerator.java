/*
 * twitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package TwitterScanner;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import com.continuuity.api.flow.flowlet.builders.TupleSchemaBuilder;
import twitter4j.*;

/**
 * TwitterGenerator is a simple SourceFlowlet that pulls tweets from the Twitter
 * public timeline, wraps them in a Tuple and sends them to the next Flowlet
 * in our Flow.
 */
public class TwitterGenerator extends AbstractSourceFlowlet {

  /**
   * This is the Twitter Client we will use to generate Tuples
   */
  private static Twitter twitterClient;

  /**
   * Configures a flowlet's streams. Basically, we define what our output
   * schema is going to be so that downstream flowlets can adhere to our
   * contract.
   *
   * @param configurator Stream configuration
   */
  @Override
  public void configure(StreamsConfigurator configurator) {

    // Create our twitter client object
    twitterClient = new TwitterFactory().getInstance();

    // Configure the Schema for our output stream
    TupleSchema mySchema =
      new TupleSchemaBuilder().add("Tweet", Status.class).create();

    // Now add it to my output stream
    configurator.getDefaultTupleOutputStream().setSchema(mySchema);
  }

  /**
   * This is the meat of this Flowlet. This is where we pull Tweets from
   * the Twitter public timeline, wrap them in Tuples and send them to
   * the next Flowlet(s) in our Flow.
   *
   * @param outputCollector  The Collector we'll use to send Tuples
   */
  @Override
  public void generate(OutputCollector outputCollector) {

    try {

      // Now, retrieve all the public timeline tweets
      ResponseList<Status> tweets = twitterClient.getPublicTimeline();

      // Walk through the tweets, wrap them in a Tuple and emit
      for (Status theStatus: tweets) {
        System.out.println(" Received a tweet of " + theStatus);

        // Remember, the Tuple's schema must match the one we established
        // in Configure
        Tuple theTuple =
          new TupleBuilder().set("Tweet", theStatus).create();

        outputCollector.emit(theTuple);
      }

    } catch (TwitterException e) {
      e.printStackTrace();
    }

  }

}
