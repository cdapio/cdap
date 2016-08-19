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

package co.cask.cdap.internal.app.runtime.batch.dataset.output;

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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Test case that tests ability to write to multiple outputs of a MapReduce job.
 */
public class MapReduceWithMultipleOutputsTest extends MapReduceRunnerTestBase {

  @Test
  public void testMultipleOutputs() throws Exception {
    ApplicationWithPrograms app = deployApp(AppWithMapReduceUsingMultipleOutputs.class);

    final FileSet fileSet = datasetCache.getDataset(AppWithMapReduceUsingMultipleOutputs.PURCHASES);
    Location inputFile = fileSet.getBaseLocation().append("inputFile");
    inputFile.createNew();

    PrintWriter writer = new PrintWriter(inputFile.getOutputStream());
    // the PURCHASES dataset consists of purchase records in the format: <customerId> <spend>
    writer.println("1 20");
    writer.println("1 65");
    writer.println("1 30");
    writer.println("2 5");
    writer.println("2 53");
    writer.println("2 45");
    writer.println("3 101");
    writer.close();

    // Using multiple outputs, this MapReduce send the records to a different path of the same dataset, depending
    // on the value in the data (large spend amounts will go to one file, while small will go to another file.
    runProgram(app, AppWithMapReduceUsingMultipleOutputs.SeparatePurchases.class, new BasicArguments());
    FileSet outputFileSet = datasetCache.getDataset(AppWithMapReduceUsingMultipleOutputs.SEPARATED_PURCHASES);

    Assert.assertEquals(ImmutableList.of("1 20", "1 30", "2 5", "2 45"),
                        readFromOutput(outputFileSet, "small_purchases"));

    Assert.assertEquals(ImmutableList.of("1 65", "2 53", "3 101"),
                        readFromOutput(outputFileSet, "large_purchases"));
  }

  private List<String> readFromOutput(FileSet fileSet, String relativePath) throws IOException {
    // small amount of data, so expect all data from just 1 file
    Location location = fileSet.getLocation(relativePath).append("part-m-00000");
    return CharStreams.readLines(CharStreams.newReaderSupplier(Locations.newInputSupplier(location), Charsets.UTF_8));
  }

  @Test
  public void testAddingMultipleOutputsWithSameAlias() throws Exception {
    final ApplicationWithPrograms app = deployApp(AppWithMapReduceUsingMultipleOutputs.class);

    // will fail because it configured two outputs with the same alias
    Assert.assertFalse(runProgram(app,
                                  AppWithMapReduceUsingMultipleOutputs.InvalidMapReduce.class, new BasicArguments()));
  }
}
