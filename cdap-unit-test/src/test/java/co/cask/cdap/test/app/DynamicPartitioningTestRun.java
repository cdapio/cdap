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

package co.cask.cdap.test.app;

import co.cask.cdap.api.dataset.InstanceNotFoundException;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetProperties;
import co.cask.cdap.api.dataset.lib.Partitioning;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.SlowTests;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import com.google.common.collect.ImmutableMap;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@Category(SlowTests.class)
public class DynamicPartitioningTestRun extends TestFrameworkTestBase {

  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", true);

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private final NamespaceId testSpace = new NamespaceId("testspace");

  @Before
  public void setUp() throws Exception {
    getNamespaceAdmin().create(new NamespaceMeta.Builder().setName(testSpace).build());
  }

  @Test
  public void testDynamicPartitioningWithFailure() throws Exception {
    // deploy app
    ApplicationManager appManager = deployApplication(testSpace, AppWithDynamicPartitioning.class);
    // setup inputs
    DataSetManager<KeyValueTable> dataSetManager = getDataset(testSpace.dataset("input"));
    KeyValueTable input = dataSetManager.get();
    for (int i = 0; i < 3; i++) {
      input.write(String.valueOf(i), "" + ('a' + i));
    }
    dataSetManager.flush();

    // run MR with one pfs
    testDynamicPartitioningMRWithFailure(appManager, "pfs1", "pfs1");
    // run MR with two pfs
    testDynamicPartitioningMRWithFailure(appManager, "pfs1", "pfs1", "pfs2");
    // run MR with two pfs in reverse order (because we don't want to rely on which one gets committed first)
    testDynamicPartitioningMRWithFailure(appManager, "pfs2", "pfs1", "pfs2");
  }

  private static final Partitioning PARTITIONING = Partitioning.builder().addStringField("x").build();

  private void testDynamicPartitioningMRWithFailure(ApplicationManager appManager,
                                                    String dsWithExistingPartition, String ... outputs)
    throws Exception {

    // set up the output datasets
    String outputArg = "";
    for (String dataset : outputs) {
      outputArg += dataset + " ";
      try {
        deleteDatasetInstance(testSpace.dataset(dataset));
      } catch (InstanceNotFoundException e) {
        // may be expected. I wish the test framework had truncate()
      }
      addDatasetInstance(PartitionedFileSet.class.getName(), testSpace.dataset(dataset),
                    PartitionedFileSetProperties.builder()
                      .setPartitioning(PARTITIONING)
                      .setEnableExploreOnCreate(true)
                      .setOutputFormat(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.class)
                      .setOutputProperty(org.apache.hadoop.mapreduce.lib.output.TextOutputFormat.SEPERATOR, ",")
                      .setExploreFormat("csv")
                      .setExploreSchema("key string, value string")
                      .build());
    }
    outputArg = outputArg.trim();

    // create partition (x="1") in one of the outputs
    DataSetManager<PartitionedFileSet> pfs = getDataset(testSpace.dataset(dsWithExistingPartition));
    Location loc = pfs.get().getEmbeddedFileSet().getLocation("some/path");
    OutputStream os = loc.append("part1").getOutputStream();
    try (Writer writer = new OutputStreamWriter(os)) {
      writer.write("1,x\n");
    }
    pfs.get().addPartition(PartitionKey.builder().addStringField("x", "1").build(), "some/path");
    pfs.flush();

    validatePartitions(dsWithExistingPartition, true);

    Map<String, String> arguments = ImmutableMap.of("outputs", outputArg);
    MapReduceManager mrManager = appManager.getMapReduceManager("DynamicPartitioningMR");
    int numRuns = mrManager.getHistory(ProgramRunStatus.FAILED).size();
    mrManager.start(arguments);
    Tasks.waitFor(numRuns + 1, () -> mrManager.getHistory(ProgramRunStatus.FAILED).size(), 300, TimeUnit.SECONDS);

    for (String dataset : outputs) {
      validatePartitions(dataset, dataset.equals(dsWithExistingPartition));
      validateFiles(dataset, dataset.equals(dsWithExistingPartition) ? loc : null);
    }
  }

  private void validatePartitions(String dataset, boolean expectPartition1) throws Exception {
    Connection connection = getQueryClient(testSpace);
    ResultSet results = connection
      .prepareStatement("SELECT key,value FROM dataset_" + dataset)
      .executeQuery();

    // should return only one or no row
    if (expectPartition1) {
      Assert.assertTrue(results.next());
      Assert.assertEquals("1", results.getString(1));
      Assert.assertEquals("x", results.getString(2));
    }
    Assert.assertFalse(results.next());
  }

  private void validateFiles(String dataset, Location expectedExisting) throws Exception {
    DataSetManager<PartitionedFileSet> pfs = getDataset(testSpace.dataset(dataset));
    Location base = pfs.get().getEmbeddedFileSet().getBaseLocation();
    validateFiles(base, expectedExisting);
  }

  private void validateFiles(Location path, Location expectedExisting) throws IOException {
    if (!path.exists()) {
      return;
    }
    if (path.equals(expectedExisting)) {
      return;
    }
    if (path.isDirectory()) {
      for (Location child : path.list()) {
        validateFiles(child, expectedExisting);
      }
    } else {
      Assert.fail("Found unexpected non-directory location: " + path.toURI().getPath());
    }
  }
}
