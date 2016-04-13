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

package co.cask.cdap.spark;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.api.dataset.lib.PartitionDetail;
import co.cask.cdap.api.dataset.lib.PartitionFilter;
import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionOutput;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.api.dataset.lib.PartitionedFileSetArguments;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSet;
import co.cask.cdap.api.dataset.lib.TimePartitionedFileSetArguments;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.spark.app.ScalaFileCountSparkProgram;
import co.cask.cdap.spark.app.SparkAppUsingFileSet;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.SparkManager;
import co.cask.cdap.test.base.TestFrameworkTestBase;
import co.cask.tephra.TransactionFailureException;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Unit-tests for testing FileSet usages in Spark program.
 */
public class SparkFileSetTestRun extends TestFrameworkTestBase {

  private ApplicationManager applicationManager;

  @Before
  public void init() {
    applicationManager = deployApplication(SparkAppUsingFileSet.class);
  }

  @Test
  public void testSparkWithFileSet() throws Exception {
    testSparkWithFileSet(applicationManager, FileCountSparkProgram.class.getSimpleName());
    testSparkWithFileSet(applicationManager, ScalaFileCountSparkProgram.class.getSimpleName());
  }

  @Test
  public void testSparkWithCustomFileSet() throws Exception {
    testSparkWithCustomFileSet(applicationManager, FileCountSparkProgram.class.getSimpleName());
    testSparkWithCustomFileSet(applicationManager, ScalaFileCountSparkProgram.class.getSimpleName());
  }

  @Test
  public void testSparkWithTimePartitionedFileSet() throws Exception {
    testSparkWithTimePartitionedFileSet(applicationManager, FileCountSparkProgram.class.getSimpleName());
    testSparkWithTimePartitionedFileSet(applicationManager, ScalaFileCountSparkProgram.class.getSimpleName());
  }

  @Test
  public void testSparkWithPartitionedFileSet() throws Exception {
    testSparkWithPartitionedFileSet(applicationManager, FileCountSparkProgram.class.getSimpleName());
    testSparkWithPartitionedFileSet(applicationManager, ScalaFileCountSparkProgram.class.getSimpleName());
  }

  private void testSparkWithFileSet(ApplicationManager applicationManager, String sparkProgram) throws Exception {
    DataSetManager<FileSet> filesetManager = getDataset("fs");
    final FileSet fileset = filesetManager.get();

    Location location = fileset.getLocation("nn");
    prepareFileInput(location);

    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "nn");
    Map<String, String> outputArgs = new HashMap<>();
    FileSetArguments.setOutputPath(inputArgs, "xx");
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "fs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "fs", outputArgs));
    args.put("input", "fs");
    args.put("output", "fs");

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram).start(args);
    sparkManager.waitForFinish(1, TimeUnit.MINUTES);
    Assert.assertEquals(1, sparkManager.getHistory(ProgramRunStatus.COMPLETED).size());

    validateFileOutput(fileset.getLocation("xx"), "custom:");

    // Cleanup paths after running test
    fileset.getLocation("nn").delete(true);
    fileset.getLocation("xx").delete(true);
  }

  private void testSparkWithCustomFileSet(ApplicationManager applicationManager,
                                          String sparkProgram) throws Exception {
    final DataSetManager<SparkAppUsingFileSet.MyFileSet> myFileSetManager = getDataset("myfs");
    SparkAppUsingFileSet.MyFileSet myfileset = myFileSetManager.get();

    final FileSet fileset = myfileset.getEmbeddedFileSet();

    Location location = fileset.getLocation("nn");
    prepareFileInput(location);

    Map<String, String> inputArgs = new HashMap<>();
    FileSetArguments.setInputPath(inputArgs, "nn");
    Map<String, String> outputArgs = new HashMap<>();
    FileSetArguments.setOutputPath(inputArgs, "xx");
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "myfs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "myfs", outputArgs));
    args.put("input", "myfs");
    args.put("output", "myfs");

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram).start(args);
    sparkManager.waitForFinish(2, TimeUnit.MINUTES);
    Assert.assertEquals(1, sparkManager.getHistory(ProgramRunStatus.COMPLETED).size());

    validateFileOutput(fileset.getLocation("xx"));

    // verify that onSuccess() was called and onFailure() was not
    Assert.assertTrue(myfileset.getSuccessLocation().exists());
    Assert.assertFalse(myfileset.getFailureLocation().exists());
    myfileset.getSuccessLocation().delete();

    // run the program again. It should fail due to existing output.
    sparkManager = applicationManager.getSparkManager(sparkProgram).start(args);
    sparkManager.waitForFinish(2, TimeUnit.MINUTES);
    Assert.assertEquals(1, sparkManager.getHistory(ProgramRunStatus.FAILED).size());

    // Then we can verify that onFailure() was called.
    Assert.assertFalse(myfileset.getSuccessLocation().exists());
    Assert.assertTrue(myfileset.getFailureLocation().exists());

    // Cleanup the paths after running the Spark program
    fileset.getLocation("nn").delete(true);
    fileset.getLocation("xx").delete(true);
    myfileset.getSuccessLocation().delete(true);
    myfileset.getFailureLocation().delete(true);
  }

  private void testSparkWithTimePartitionedFileSet(ApplicationManager applicationManager,
                                                   String sparkProgram) throws Exception {
    long customOutputPartitionKey = 123456789L;
    long customInputPartitionKey = 987654321L;

    DataSetManager<TimePartitionedFileSet> tpfsManager = getDataset("tpfs");
    long inputTime = System.currentTimeMillis();
    long outputTime = inputTime + TimeUnit.HOURS.toMillis(1);

    addTimePartition(tpfsManager, inputTime);
    addTimePartition(tpfsManager, customInputPartitionKey);

    Map<String, String> inputArgs = new HashMap<>();
    TimePartitionedFileSetArguments.setInputStartTime(inputArgs, inputTime - 100);
    TimePartitionedFileSetArguments.setInputEndTime(inputArgs, inputTime + 100);
    Map<String, String> outputArgs = new HashMap<>();
    TimePartitionedFileSetArguments.setOutputPartitionTime(outputArgs, outputTime);
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "tpfs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "tpfs", outputArgs));
    args.put("input", "tpfs");
    args.put("output", "tpfs");
    args.put("outputKey", String.valueOf(customOutputPartitionKey));
    args.put("inputKey", String.valueOf(customInputPartitionKey));

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram).start(args);
    sparkManager.waitForFinish(10, TimeUnit.MINUTES);
    Assert.assertEquals(1, sparkManager.getHistory(ProgramRunStatus.COMPLETED).size());

    tpfsManager.flush();
    TimePartitionedFileSet tpfs = tpfsManager.get();

    PartitionDetail partition = tpfs.getPartitionByTime(outputTime);
    Assert.assertNotNull("Output partition is null while for running without custom dataset arguments",
                         partition);
    validateFileOutput(partition.getLocation());

    PartitionDetail customPartition = tpfs.getPartitionByTime(customOutputPartitionKey);
    Assert.assertNotNull("Output partition is null while for running with custom dataset arguments",
                         customPartition);
    validateFileOutput(customPartition.getLocation());

    // Cleanup after running the test
    tpfs.getPartitionOutput(inputTime).getLocation().delete(true);
    tpfs.getPartitionOutput(customInputPartitionKey).getLocation().delete(true);
    partition.getLocation().delete(true);
    customPartition.getLocation().delete(true);

    tpfs.dropPartition(inputTime);
    tpfs.dropPartition(customInputPartitionKey);
    tpfs.dropPartition(partition.getPartitionKey());
    tpfs.dropPartition(customPartition.getPartitionKey());

    tpfsManager.flush();
  }

  private void testSparkWithPartitionedFileSet(ApplicationManager applicationManager,
                                               String sparkProgram) throws Exception {
    DataSetManager<PartitionedFileSet> pfsManager = getDataset("pfs");
    PartitionedFileSet pfs = pfsManager.get();
    PartitionOutput partitionOutput = pfs.getPartitionOutput(PartitionKey.builder().addStringField("x", "nn").build());
    Location location = partitionOutput.getLocation();
    prepareFileInput(location);
    partitionOutput.addPartition();
    pfsManager.flush();

    Map<String, String> inputArgs = new HashMap<>();
    PartitionedFileSetArguments.setInputPartitionFilter(
      inputArgs, PartitionFilter.builder().addRangeCondition("x", "na", "nx").build());
    Map<String, String> outputArgs = new HashMap<>();
    final PartitionKey outputKey = PartitionKey.builder().addStringField("x", "xx").build();
    PartitionedFileSetArguments.setOutputPartitionKey(outputArgs, outputKey);
    Map<String, String> args = new HashMap<>();
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "pfs", inputArgs));
    args.putAll(RuntimeArguments.addScope(Scope.DATASET, "pfs", outputArgs));
    args.put("input", "pfs");
    args.put("output", "pfs");

    SparkManager sparkManager = applicationManager.getSparkManager(sparkProgram).start(args);
    sparkManager.waitForFinish(10, TimeUnit.MINUTES);
    Assert.assertEquals(1, sparkManager.getHistory(ProgramRunStatus.COMPLETED).size());

    pfsManager.flush();
    PartitionDetail partition = pfs.getPartition(outputKey);
    Assert.assertNotNull(partition);
    validateFileOutput(partition.getLocation());

    // Cleanup after test completed
    location.delete(true);
    partition.getLocation().delete(true);
    pfs.dropPartition(partitionOutput.getPartitionKey());
    pfs.dropPartition(partition.getPartitionKey());

    pfsManager.flush();
  }

  private void prepareFileInput(Location location) throws IOException {
    try (OutputStreamWriter out = new OutputStreamWriter(location.getOutputStream())) {
      out.write("13 characters\n");
      out.write("7 chars\n");
    }
  }

  private void validateFileOutput(Location location) throws Exception {
    validateFileOutput(location, "");
  }

  private void validateFileOutput(Location location, String prefix) throws Exception {
    Assert.assertTrue(location.isDirectory());
    for (Location child : location.list()) {
      if (child.getName().startsWith("part-r-")) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(child.getInputStream()))) {
          String line = reader.readLine();
          Assert.assertNotNull(line);
          Assert.assertEquals(prefix + "13 characters:13", line);
          line = reader.readLine();
          Assert.assertNotNull(line);
          Assert.assertEquals(prefix + "7 chars:7", line);
          line = reader.readLine();
          Assert.assertNull(line);
          return;
        }
      }
    }
    Assert.fail("Output directory does not contain any part file: " + location.list());
  }

  private void addTimePartition(DataSetManager<TimePartitionedFileSet> tpfsManager,
                                long inputTime) throws IOException, TransactionFailureException, InterruptedException {
    TimePartitionedFileSet tpfs = tpfsManager.get();

    PartitionOutput partitionOutput = tpfs.getPartitionOutput(inputTime);
    Location location = partitionOutput.getLocation();
    prepareFileInput(location);
    partitionOutput.addPartition();

    tpfsManager.flush();
  }
}
