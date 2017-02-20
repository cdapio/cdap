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

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests that a MapReduce job can process data from two different Streams and perform a reduce-side join across
 * the data in the two Streams.
 */
public class ClicksAndViewsMapReduceTest extends TestBase {
  private static final Joiner TAB_JOINER = Joiner.on("\t");

  // have views with id [0,5], all for the same adId
  private static final ImmutableList<String> VIEWS =
    ImmutableList.of(createView(0, 1461219010, 2157, "http://www.google.com", "lu=fQ9qHjLjFg3qi3bZiuz", "62.128.93.36"),
                     createView(1, 1461265001, 2157, "http://www.google.co.uk", "lu=8fsdggknea@ASJHlz", "21.612.39.63"),
                     createView(2, 1461281958, 2157, "http://www.yahoo.com", "name=Mike", "212.193.252.52"),
                     createView(3, 1461331879, 2157, "http://www.amazon.com", "name=Matt", "1.116.135.146"),
                     createView(4, 1461348738, 2157, "http://www.t.co", "name=Nicholas; Httponly", "89.141.94.158"),
                     createView(5, 1461349158, 2157, "http://www.linkedin.com", "lo=Npa0jbIHGloMnx75", "69.75.87.114"));

  private static final ImmutableList<Integer> CLICKS = ImmutableList.of(1, 2, 5);

  private static final int OUTPUT_PARTITION_RUNTIME = 1461280019;

  @Test
  public void testClicksAndViews() throws Exception {
    ApplicationManager applicationManager = deployApplication(ClicksAndViews.class);

    // write each of the views to the VIEWS stream
    StreamManager viewsStream = getStreamManager(ClicksAndViews.VIEWS);
    for (String view : VIEWS) {
      viewsStream.send(view);
    }

    // send clicks for viewIds 1,2,5
    StreamManager clicksStream = getStreamManager(ClicksAndViews.CLICKS);
    for (Integer click : CLICKS) {
      clicksStream.send(createClick(click));
    }

    MapReduceManager mapReduceManager = applicationManager.getMapReduceManager(ClicksAndViewsMapReduce.NAME)
      // configure this run of the MapReduce to write to the partition keyed by OUTPUT_PARTITION_RUNTIME
      .start(ImmutableMap.of("output.partition.key.runtime", Integer.toString(OUTPUT_PARTITION_RUNTIME)));

    mapReduceManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    List<String> joinedViews = new ArrayList<>();
    for (int i = 0; i < VIEWS.size(); i++) {
      joinedViews.add(createJoinedView(VIEWS.get(i), Collections.frequency(CLICKS, i)));
    }
    Set<String> expectedJoinedViews = ImmutableSet.copyOf(joinedViews);
    Assert.assertEquals(expectedJoinedViews, getDataFromFile());
    Assert.assertEquals(expectedJoinedViews, getDataFromExplore());
  }

  private static String createView(int viewId, long requestBeginTime, int adId, String referrer,
                                   String userCookie, String ip) {
    // View:
    // viewId, requestBeginTime, adId, referrer, userCookie, ip
    return TAB_JOINER.join(viewId, requestBeginTime, adId, referrer, userCookie, ip);
  }

  private static String createClick(int viewId) {
    // Click:
    // viewId
    return Integer.toString(viewId);
  }

  private static String createJoinedView(String view, int numClicks) {
    // Joined View:
    // viewId, requestBeginTime, adId, referrer, userCookie, ip, numClicks
    return TAB_JOINER.join(view, numClicks);
  }

  private Set<String> getDataFromFile() throws Exception {
    DataSetManager<PartitionedFileSet> cleanRecords = getDataset(ClicksAndViews.JOINED);
    Set<String> cleanData = new HashSet<>();
    // we configured the MapReduce to write to this partition when starting it
    PartitionDetail partition =
      cleanRecords.get().getPartition(PartitionKey.builder().addLongField("runtime", OUTPUT_PARTITION_RUNTIME).build());
    Assert.assertNotNull(partition);
    for (Location location : partition.getLocation().list()) {
      if (location.getName().startsWith("part-")) {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(location.getInputStream()))) {
          String line;
          while ((line = bufferedReader.readLine()) != null) {
            cleanData.add(line);
          }
        }
      }
    }
    return cleanData;
  }

  private Set<String> getDataFromExplore() throws Exception {
    try (Connection connection = getQueryClient()) {
      ResultSet results = connection
        .prepareStatement("SELECT * FROM dataset_" + ClicksAndViews.JOINED)
        .executeQuery();

      Set<String> cleanRecords = new HashSet<>();
      while (results.next()) {
        cleanRecords.add(TAB_JOINER.join(results.getString(1), results.getString(2), results.getString(3),
                                         results.getString(4), results.getString(5), results.getString(6),
                                         results.getString(7)));
      }
      return cleanRecords;
    }
  }
}
