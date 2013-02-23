/*
 * TwitterScanner - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.examples.twitter;

import com.continuuity.api.annotation.UseDataSet;
import com.continuuity.api.common.Bytes;
import com.continuuity.api.flow.flowlet.AbstractFlowlet;
import com.continuuity.api.flow.flowlet.FlowletSpecification;

import java.util.Map;

public class TwitterWordIndexer extends AbstractFlowlet {

  @UseDataSet(TwitterFlow.topUsers)
  private SortedCounterTable topUsers;

  public TwitterWordIndexer() {
    super("TwitterWordIndexer");
  }

  public FlowletSpecification configure() {
    return FlowletSpecification.Builder.with()
      .setName(getName())
      .setDescription(getDescription())
      .useDataSet(TwitterFlow.topUsers)
      .build();
  }

  public void process(Map<String,Object> tuple) {
    String user = (String) tuple.get("name");
    Long postValue = (Long) tuple.get("value");

    // Perform post-increment for top users
    topUsers.performSecondaryCounterIncrements(
        TwitterFlow.USER_SET, Bytes.toBytes(user), 1L, postValue);
  }
}
