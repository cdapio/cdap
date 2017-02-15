/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package co.cask.cdap.examples.datacleansing;

import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.cube.AggregationFunction;
import co.cask.cdap.api.dataset.lib.cube.TimeValue;
import co.cask.cdap.api.metrics.MetricDataQuery;
import co.cask.cdap.api.metrics.MetricTimeSeries;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Tests that a MapReduce job can incrementally process the partitions of a PartitionedFileSet, using a small sample of
 * data with the DataCleansing MapReduce job.
 */
public class DataCleansingMapReduceTest extends TestBase {
  private static final String RECORD1 =
    "{\"pid\":223986723,\"name\":\"bob\",\"dob\":\"02-12-1983\",\"zip\":\"84125\"}";
  private static final String RECORD2 =
    "{\"pid\":198637201,\"name\":\"timothy\",\"dob\":\"06-21-1995\",\"zip\":\"84125q\"}";
  private static final Set<String> RECORD_SET1 = ImmutableSet.of(RECORD1, RECORD2);

  private static final String RECORD3 =
    "{\"pid\":001058370,\"name\":\"jill\",\"dob\":\"12-12-1963\",\"zip\":\"84126\"}";
  private static final String RECORD4 =
    "{\"pid\":000150018,\"name\":\"wendy\",\"dob\":\"06-19-1987\",\"zip\":\"84125\"}";
  private static final Set<String> RECORD_SET2 = ImmutableSet.of(RECORD3, RECORD4);

  private static final String RECORD5 =
    "{\"pid\":013587810,\"name\":\"john\",\"dob\":\"10-10-1991\",\"zip\":\"84126\"}";
  private static final String RECORD6 =
    "{\"pid\":811638015,\"name\":\"samantha\",\"dob\":\"04-20-1965\",\"zip\":\"84125\"}";
  private static final Set<String> RECORD_SET3 = ImmutableSet.of(RECORD5, RECORD6);

  private static final String schemaJson = DataCleansingMapReduce.SchemaMatchingFilter.DEFAULT_SCHEMA.toString();
  private static final SimpleSchemaMatcher schemaMatcher =
    new SimpleSchemaMatcher(DataCleansingMapReduce.SchemaMatchingFilter.DEFAULT_SCHEMA);

  @Test
  public void testPartitionConsuming() throws Exception {
    ApplicationManager applicationManager = deployApplication(DataCleansing.class);

    ServiceManager serviceManager = applicationManager.getServiceManager(DataCleansingService.NAME).start();
    serviceManager.waitForStatus(true);
    URL serviceURL = serviceManager.getServiceURL();

    // write a set of records to one partition and run the DataCleansingMapReduce job on that one partition
    createPartition(serviceURL, RECORD_SET1);

    // before starting the MR, there are 0 invalid records and 0 valid records, according to metrics
    Assert.assertEquals(0, getValidityMetrics(true));
    Assert.assertEquals(0, getValidityMetrics(false));
    Long now = System.currentTimeMillis();
    ImmutableMap<String, String> args = ImmutableMap.of(DataCleansingMapReduce.OUTPUT_PARTITION_KEY, now.toString(),
                                                        DataCleansingMapReduce.SCHEMA_KEY, schemaJson);
    MapReduceManager mapReduceManager = applicationManager.getMapReduceManager(DataCleansingMapReduce.NAME).start(args);
    mapReduceManager.waitForRun(ProgramRunStatus.COMPLETED, 5, TimeUnit.MINUTES);

    compareData(now, DataCleansing.CLEAN_RECORDS, filterRecords(RECORD_SET1, true));
    compareData(now, DataCleansing.INVALID_RECORDS, filterRecords(RECORD_SET1, false));

    // assert that some of the records have indeed been filtered
    Assert.assertNotEquals(filterRecords(RECORD_SET1, true), RECORD_SET1);
    Assert.assertNotEquals(filterRecords(RECORD_SET1, false), Collections.<String>emptySet());

    // verify this via metrics
    Assert.assertEquals(1, getValidityMetrics(true));
    Assert.assertEquals(1, getValidityMetrics(false));

    // create two additional partitions
    createPartition(serviceURL, RECORD_SET2);
    createPartition(serviceURL, RECORD_SET3);

    // running the MapReduce job now processes these two new partitions (RECORD_SET1 and RECORD_SET2) and creates a new
    // partition with with the output
    now = System.currentTimeMillis();
    args = ImmutableMap.of(DataCleansingMapReduce.OUTPUT_PARTITION_KEY, now.toString(),
                           DataCleansingMapReduce.SCHEMA_KEY, schemaJson);

    mapReduceManager = applicationManager.getMapReduceManager(DataCleansingMapReduce.NAME).start(args);
    mapReduceManager.waitForRuns(ProgramRunStatus.COMPLETED, 2, 5, TimeUnit.MINUTES);

    ImmutableSet<String> recordSets2and3 =
      ImmutableSet.<String>builder().addAll(RECORD_SET2).addAll(RECORD_SET3).build();
    compareData(now, DataCleansing.CLEAN_RECORDS, filterRecords(recordSets2and3, true));
    compareData(now, DataCleansing.INVALID_RECORDS, filterRecords(recordSets2and3, false));

    // verify this via metrics
    Assert.assertEquals(1, getValidityMetrics(true));
    Assert.assertEquals(5, getValidityMetrics(false));


    // running the MapReduce job without adding new partitions creates no additional output
    now = System.currentTimeMillis();
    args = ImmutableMap.of(DataCleansingMapReduce.OUTPUT_PARTITION_KEY, now.toString(),
                           DataCleansingMapReduce.SCHEMA_KEY, schemaJson);

    mapReduceManager = applicationManager.getMapReduceManager(DataCleansingMapReduce.NAME).start(args);
    mapReduceManager.waitForRuns(ProgramRunStatus.COMPLETED, 3, 5, TimeUnit.MINUTES);

    compareData(now, DataCleansing.CLEAN_RECORDS, Collections.<String>emptySet());
    compareData(now, DataCleansing.INVALID_RECORDS, Collections.<String>emptySet());

    // verify that the records were properly partitioned on their zip
    DataSetManager<PartitionedFileSet> cleanRecords = getDataset(DataCleansing.CLEAN_RECORDS);
    PartitionFilter filter = PartitionFilter.builder().addValueCondition("zip", 84125).build();
    Assert.assertEquals(ImmutableSet.of(RECORD1, RECORD4, RECORD6), getDataFromFilter(cleanRecords.get(), filter));

    filter = PartitionFilter.builder().addValueCondition("zip", 84126).build();
    Assert.assertEquals(ImmutableSet.of(RECORD3, RECORD5), getDataFromFilter(cleanRecords.get(), filter));
  }

  private void createPartition(URL serviceUrl, Set<String> records) throws IOException {
    URL url = new URL(serviceUrl, "v1/records/raw");
    String body = Joiner.on("\n").join(records) + "\n";
    HttpRequest request = HttpRequest.post(url).withBody(body).build();
    HttpResponse response = HttpRequests.execute(request);
    Assert.assertEquals(200, response.getResponseCode());
  }

  private void compareData(Long time, String dsName, Set<String> expectedRecords) throws Exception {
    Assert.assertEquals(expectedRecords, getDataFromFile(time, dsName));
    Assert.assertEquals(expectedRecords, getDataFromExplore(time, dsName));
  }

  private Set<String> getDataFromExplore(Long time, String dsName) throws Exception {
    try (Connection connection = getQueryClient()) {
      ResultSet results = connection
        .prepareStatement("SELECT * FROM dataset_" + dsName + " where TIME = " + time)
        .executeQuery();

      Set<String> cleanRecords = new HashSet<>();
      while (results.next()) {
        cleanRecords.add(results.getString(1));
      }
      return cleanRecords;
    }
  }

  private Set<String> getDataFromFile(Long time, String dsName) throws Exception {
    DataSetManager<PartitionedFileSet> cleanRecords = getDataset(dsName);
    PartitionFilter filter = PartitionFilter.builder().addValueCondition("time", time).build();
    return getDataFromFilter(cleanRecords.get(), filter);
  }

  private Set<String> getDataFromFilter(PartitionedFileSet partitionedFileSet,
                                        PartitionFilter filter) throws IOException {
    Set<PartitionDetail> partitions = partitionedFileSet.getPartitions(filter);
    Set<String> cleanData = new HashSet<>();
    for (PartitionDetail partition : partitions) {
      Assert.assertEquals(ImmutableMap.of("source.program", "DataCleansingMapReduce"),
                          partition.getMetadata().asMap());
      Location partitionLocation = partition.getLocation();
      for (Location location : partitionLocation.list()) {
        if (location.getName().startsWith("part-")) {
          try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(location.getInputStream()))) {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
              cleanData.add(line);
            }
          }
        }
      }
    }
    return cleanData;
  }

  /**
   * @param records the set of records to filter
   * @param filterInvalids if true, will filter out invalid records; else, will return only invalid records
   * @return the filtered set of records
   */
  private Set<String> filterRecords(Set<String> records, boolean filterInvalids) {
    Set<String> filteredSet = new HashSet<>();
    for (String record : records) {
      if (filterInvalids == schemaMatcher.matches(record)) {
        filteredSet.add(record);
      }
    }
    return filteredSet;
  }

  // pass true to get the number of invalid records; pass false to get the number of valid records processed.
  private long getValidityMetrics(boolean invalid) throws Exception {
    String metric = "user.records." + (invalid ? "invalid" : "valid");

    Map<String, String> tags = ImmutableMap.of(Constants.Metrics.Tag.NAMESPACE, NamespaceId.DEFAULT.getNamespace(),
                                               Constants.Metrics.Tag.APP, DataCleansing.NAME,
                                               Constants.Metrics.Tag.MAPREDUCE, DataCleansingMapReduce.NAME);
    MetricDataQuery metricQuery = new MetricDataQuery(0, Integer.MAX_VALUE, Integer.MAX_VALUE, metric,
                                                      AggregationFunction.SUM, tags, ImmutableList.<String>of());
    Collection<MetricTimeSeries> result = getMetricsManager().query(metricQuery);

    if (result.isEmpty()) {
      return 0;
    }

    // since it is totals query and not groupBy specified, we know there's one time series
    List<TimeValue> timeValues = result.iterator().next().getTimeValues();
    if (timeValues.isEmpty()) {
      return 0;
    }

    // since it is totals, we know there's one value only
    return timeValues.get(0).getValue();
  }
}
