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
import co.cask.cdap.api.dataset.lib.TimePartitionDetail;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.StreamManager;
import co.cask.cdap.test.TestBase;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Calendar;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicMarkableReference;

/**
 * Test the stream conversion example app.
 */
public class StreamConversionTest extends TestBase {

  @Test
  public void testStreamConversion() throws Exception {

    // Deploy the PurchaseApp application
    ApplicationManager appManager = deployApplication(StreamConversionApp.class);

    // send some data to the events stream
    StreamManager streamManager = getStreamManager("events");
    streamManager.send("15");
    streamManager.send("16");
    streamManager.send("17");

    // record the current time
    final long startTime = System.currentTimeMillis();

    // run the mapreduce
    MapReduceManager mapReduceManager =
      appManager.getMapReduceManager("StreamConversionMapReduce").start(RuntimeArguments.NO_ARGUMENTS);
    mapReduceManager.waitForFinish(5, TimeUnit.MINUTES);

    // verify the single partition in the file set
    long partitionTime = assertWithRetry(new Callable<Long>() {
      @Override
      public Long call() throws Exception {
        DataSetManager<TimePartitionedFileSet> fileSetManager = getDataset("converted");
        TimePartitionedFileSet converted = fileSetManager.get();
        Set<TimePartitionDetail> partitions = converted.getPartitionsByTime(startTime, System.currentTimeMillis());
        Assert.assertEquals(1, partitions.size());
        return partitions.iterator().next().getTime();
      }
    }, 15L, TimeUnit.SECONDS, 100L, TimeUnit.MILLISECONDS);

    // we must round down the start time to the full minute before we compare the partition time
    Calendar calendar = Calendar.getInstance();
    calendar.setTimeInMillis(startTime);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    long startMinute = calendar.getTimeInMillis();

    // partition time should be the logical start time of the MapReduce. That is between start and now.
    Assert.assertTrue(partitionTime >= startMinute);
    Assert.assertTrue(partitionTime <= System.currentTimeMillis());

    // extract fields from partition time
    calendar.setTimeInMillis(partitionTime);
    int year = calendar.get(Calendar.YEAR);
    int month = calendar.get(Calendar.MONTH) + 1;
    int day = calendar.get(Calendar.DAY_OF_MONTH);
    int hour = calendar.get(Calendar.HOUR_OF_DAY);
    int minute = calendar.get(Calendar.MINUTE);

    // query with SQL
    Connection connection = getQueryClient();
    ResultSet results = connection.prepareStatement("SELECT year, month, day, hour, minute " +
                                                      "FROM dataset_converted " +
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

  @Test
  public void testAssertWithRetry() throws InterruptedException, ExecutionException, TimeoutException {
    final int requiredCount = 10;
    final AtomicInteger count = new AtomicInteger(0);
    int actualCount = assertWithRetry(new Callable<Integer>() {
      @Override
      public Integer call() throws Exception {
        int actualCount = count.getAndIncrement();
        Assert.assertEquals(requiredCount, actualCount);
        return actualCount;
      }
    }, 10, TimeUnit.SECONDS, 0, TimeUnit.SECONDS);
    Assert.assertEquals(requiredCount, actualCount);
  }

  // TODO: move elsewhere?
  private <T> T assertWithRetry(final Callable<T> callable, long timeout, TimeUnit timeoutUnit,
                        long sleepDelay, TimeUnit sleepDelayUnit)
    throws InterruptedException, ExecutionException, TimeoutException {

    final AtomicMarkableReference<T> result = new AtomicMarkableReference<>(null, false);
    Tasks.waitFor(true, new Callable<Boolean>() {
      public Boolean call() throws Exception {
        try {
          result.set(callable.call(), true);
        } catch (AssertionError e) {
          // retry
          return false;
        }
        return true;
      }
    }, timeout, timeoutUnit, sleepDelay, sleepDelayUnit);
    Assert.assertTrue(result.isMarked());
    return result.getReference();
  }
}
