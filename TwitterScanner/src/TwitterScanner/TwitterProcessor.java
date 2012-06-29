/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package TwitterScanner;

import com.continuuity.api.flow.flowlet.*;
import com.continuuity.api.flow.flowlet.builders.TupleBuilder;
import twitter4j.Status;

public class TwitterProcessor extends AbstractComputeFlowlet {

  /**
   * Processes a Tuple at a time.
   *
   * @param tuple
   * @param context   the {@link com.continuuity.api.flow.flowlet.Tuple} came from.
   * @param collector
   */
  @Override
  public void process(Tuple tuple, TupleContext context, OutputCollector collector) {

    // Retrieve the Tweet from the tuple
    Status theTweet = tuple.get("Tweet");

    /* Who was the author of the tweet?
    String author = theTweet.getUser().getScreenName();
    String url = theTweet.getPlace().getName();
    String fav = theTweet.get

    Tuple theTuple = new TupleBuilder().set("Author")       */

  }

  /**
   * Configures a flowlet streams.
   *
   * @param configurator Streams configuration
   */
  @Override
  public void configure(StreamsConfigurator configurator) {
    //To change body of implemented methods use File | Settings | File Templates.
  }
}
