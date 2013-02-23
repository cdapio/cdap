/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import java.util.Map;

public class TwitterHashTagIndexer extends AbstractFlowlet {

  public TwitterHashTagIndexer() {
    super("HashTagIndexer");
  }
  private SortedCounterTable topHashTags;

  public void process(Map<String,Object> tuple) {

    String hashtag = (String) tuple.get("name");
    Long postValue = (Long) tuple.get("value");

    // Perform post-increment for top users
    topHashTags.performSecondaryCounterIncrements(
        TwitterFlow.HASHTAG_SET, Bytes.toBytes(hashtag), 1L, postValue);
    
  }
}
