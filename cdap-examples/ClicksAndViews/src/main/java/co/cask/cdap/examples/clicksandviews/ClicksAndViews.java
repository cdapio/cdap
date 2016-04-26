/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.examples.clicksandviews;

import co.cask.cdap.api.app.AbstractApplication;
import co.cask.cdap.api.data.stream.Stream;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Clicks and Views sample Application.
 */
public class ClicksAndViews extends AbstractApplication {
  static final String NAME = "ClicksAndViews";
  static final String VIEWS = "views";
  static final String CLICKS = "clicks";
  static final String JOINED = "joined";

  @Override
  public void configure() {
    setName(NAME);
    setDescription("Example Clicks and Views Application");

    // Process the "clicks" and "views" streams using MapReduce
    addMapReduce(new ClicksAndViewsMapReduce());

    addStream(new Stream(VIEWS, "Stores the views that happen for each ad"));
    addStream(new Stream(CLICKS, "Stores the clicks that happen for each view"));

    createDataset(JOINED, PartitionedFileSet.class, PartitionedFileSetProperties.builder()
      // partition on "runtime", represents the MapReduce's logical runtime
      .setPartitioning(Partitioning.builder().addLongField("runtime").build())
      // Property for file set to be used as output of MapReduce
      .setOutputFormat(TextOutputFormat.class)
      // Properties for Explore (to create a partitioned Hive table)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("text")
      .setExploreFormatProperty("delimiter", "\t")
      // viewId, requestBeginTime, adId, referrer, userCookie, ip, numClicks
      .setExploreSchema("viewId BIGINT, requestBeginTime BIGINT, adId BIGINT, referrer STRING, " +
                          "userCookie STRING, ip STRING, numClicks INT")
      .setDescription("Stores the joined views, which additionally keeps track of how many clicks each view had")
      .build());
  }
}
