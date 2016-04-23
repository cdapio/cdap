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

package co.cask.cdap.internal.app.runtime.batch.dataset.input;

import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.internal.app.deploy.pipeline.ApplicationWithPrograms;
import co.cask.cdap.internal.app.runtime.BasicArguments;
import co.cask.cdap.internal.app.runtime.batch.MapReduceRunnerTestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.CharStreams;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.util.List;

/**
 * Test case that tests ability to perform join across two datasets, using multiple inputs of a MapReduce job.
 */
public class MapReduceWithMultipleInputsTest extends MapReduceRunnerTestBase {

  @Test
  public void testSimpleJoin() throws Exception {
    ApplicationWithPrograms app = deployApp(AppWithMapReduceUsingMultipleInputs.class);

    final FileSet fileSet = datasetCache.getDataset(AppWithMapReduceUsingMultipleInputs.PURCHASES);
    Location inputFile = fileSet.getBaseLocation().append("inputFile");
    inputFile.createNew();

    PrintWriter writer = new PrintWriter(inputFile.getOutputStream());
    // the PURCHASES dataset consists of purchase records in the format: <customerId> <spend>
    writer.println("1 20");
    writer.println("1 25");
    writer.println("1 30");
    writer.println("2 5");
    writer.close();

    // write some of the purchases to the stream
    writeToStream(AppWithMapReduceUsingMultipleInputs.PURCHASES, "2 13");
    writeToStream(AppWithMapReduceUsingMultipleInputs.PURCHASES, "3 60");

    FileSet fileSet2 = datasetCache.getDataset(AppWithMapReduceUsingMultipleInputs.CUSTOMERS);
    inputFile = fileSet2.getBaseLocation().append("inputFile");
    inputFile.createNew();

    // the CUSTOMERS dataset consists of records in the format: <customerId> <customerName>
    writer = new PrintWriter(inputFile.getOutputStream());
    writer.println("1 Bob");
    writer.println("2 Samuel");
    writer.println("3 Joe");
    writer.close();

    // Using multiple inputs, this MapReduce will join on the two above datasets to get aggregate results.
    // The records are expected to be in the form: <customerId> <customerName> <totalSpend>
    runProgram(app, AppWithMapReduceUsingMultipleInputs.ComputeSum.class, new BasicArguments());
    FileSet outputFileSet = datasetCache.getDataset(AppWithMapReduceUsingMultipleInputs.OUTPUT_DATASET);
    // will only be 1 part file, due to the small amount of data
    Location outputLocation = outputFileSet.getBaseLocation().append("output").append("part-r-00000");

    List<String> lines = CharStreams.readLines(
      CharStreams.newReaderSupplier(Locations.newInputSupplier(outputLocation), Charsets.UTF_8));

    Assert.assertEquals(ImmutableList.of("1 Bob 75", "2 Samuel 18", "3 Joe 60"),
                        lines);

    // assert that the mapper was initialized and destroyed (this doesn't happen when using hadoop's MultipleOutputs).
    Assert.assertEquals("true", System.getProperty("mapper.initialized"));
    Assert.assertEquals("true", System.getProperty("mapper.destroyed"));
  }

  @Test
  public void testMapperOutputTypeChecking() throws Exception {
    final ApplicationWithPrograms app = deployApp(AppWithMapReduceUsingInconsistentMappers.class);

    // the mapreduce with consistent mapper types will succeed
    Assert.assertTrue(runProgram(app,
                                 AppWithMapReduceUsingInconsistentMappers.MapReduceWithConsistentMapperTypes.class,
                                 new BasicArguments()));

    // the mapreduce with mapper classes of inconsistent output types will fail, whether the mappers are set through
    // CDAP APIs or also directly on the job
    Assert.assertFalse(runProgram(app,
                                  AppWithMapReduceUsingInconsistentMappers.MapReduceWithInconsistentMapperTypes.class,
                                  new BasicArguments()));

    Assert.assertFalse(runProgram(app,
                                  AppWithMapReduceUsingInconsistentMappers.MapReduceWithInconsistentMapperTypes2.class,
                                  new BasicArguments()));
  }

  @Test
  public void testAddingMultipleInputsWithSameAlias() throws Exception {
    final ApplicationWithPrograms app = deployApp(AppWithMapReduceUsingMultipleInputs.class);

    // will fail because it configured two inputs with the same alias
    Assert.assertFalse(runProgram(app,
                                  AppWithMapReduceUsingMultipleInputs.InvalidMapReduce.class, new BasicArguments()));
  }
}
