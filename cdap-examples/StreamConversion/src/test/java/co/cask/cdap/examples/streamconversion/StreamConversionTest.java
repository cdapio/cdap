/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.examples.streamconversion;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import co.cask.cdap.test.WorkflowManager;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test the stream conversion example app.
 */
public class StreamConversionTest extends TestBase {

  @Test
  public void testStreamConversion() throws Exception {

    // Deploy the PurchaseApp application
    ApplicationManager appManager = deployApplication(StreamConversionApp.class);

    // TODO: in unit tests, all schedules should be disabled at deploy time, to avoid race conditions.
    // make sure the workflow does not get started by the schedule.
    WorkflowManager workflowManager =
      appManager.startWorkflow("StreamConversionWorkflow", RuntimeArguments.NO_ARGUMENTS);
    workflowManager.getSchedule("every5min").suspend();

    // send some data to the events stream
    StreamWriter streamWriter = appManager.getStreamWriter("events");
    streamWriter.send("15");
    streamWriter.send("16");
    streamWriter.send("17");

    // record the current time
    long startTime = System.currentTimeMillis();

    // run the workflow
    MapReduceManager mapReduceManager =
      appManager.startMapReduce("StreamConversionMapReduce", RuntimeArguments.NO_ARGUMENTS);
    mapReduceManager.waitForFinish(5, TimeUnit.MINUTES);

    // verify the single partition in the file set
    DataSetManager<TimePartitionedFileSet> fileSetManager= getDataset("converted");
    TimePartitionedFileSet converted = fileSetManager.get();
    Map<Long, String> partitions = converted.getPartitions(startTime, System.currentTimeMillis());
    Assert.assertEquals(1, partitions.size());
    long partitionTime = partitions.keySet().iterator().next();

    // partition time should be the logical start time of the MapReduce. Let's say within 1 minute after start.
    Assert.assertTrue(partitionTime >= startTime);
    Assert.assertTrue(partitionTime <= startTime + TimeUnit.SECONDS.toMillis(60));

    // extract fields from partition time
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(startTime);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1;
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int minute = calendar.get(Calendar.MINUTE);

    // query with SQL
    Connection connection = getQueryClient();
    ResultSet results = connection.prepareStatement("SELECT year, month, day, hour, minute " +
                                                      "FROM cdap_user_converted " +
                                                      "WHERE body = '17'").executeQuery();

    // should return only one row, with correct time fields
    Assert.assertTrue(results.next());
    Assert.assertEquals(year, results.getInt(1));
    Assert.assertEquals(month, results.getInt(2));
    Assert.assertEquals(day, results.getInt(3));
    Assert.assertEquals(hour, results.getInt(4));
    Assert.assertEquals(minute, results.getInt(5));
    Assert.assertFalse(results.next());
  }
}
