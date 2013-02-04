/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.ComputeFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecifier;
import com.continuuity.api.flow.flowlet.OutputCollector;
import com.continuuity.api.flow.flowlet.Tuple;
import com.continuuity.api.flow.flowlet.TupleContext;

public class TwitterHashTagIndexer extends ComputeFlowlet {

  @Override
  public void configure(FlowletSpecifier specifier) {
    specifier.getDefaultFlowletInput().setSchema(
        TwitterFlow.POST_PROCESS_SCHEMA);
  }

  private SortedCounterTable topHashTags;

  @Override
  public void initialize() {
    this.topHashTags = getFlowletContext().getDataSet(TwitterFlow.topHashTags);
  }

  @Override
  public void process(Tuple tuple, TupleContext context,
      OutputCollector collector) {

    String hashtag = tuple.get("name");
    Long postValue = tuple.get("value");

    // Perform post-increment for top users
    topHashTags.performSecondaryCounterIncrements(
        TwitterFlow.HASHTAG_SET, Bytes.toBytes(hashtag), 1L, postValue);
    
  }

}
