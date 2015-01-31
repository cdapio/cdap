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

package co.cask.cdap.examples.fileset;

import co.cask.cdap.api.common.RuntimeArguments;
import co.cask.cdap.api.common.Scope;
import co.cask.cdap.api.dataset.lib.FileSet;
import co.cask.cdap.api.dataset.lib.FileSetArguments;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.Maps;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A unit test for word count over file sets.
 */
public class FileSetWordCountTest extends TestBase {

  @Test
  public void testWordCountOnFileSet() throws Exception {

    // deploy the application
    ApplicationManager applicationManager = deployApplication(FileSetExample.class);

    final String line1 = "a b a";
    final String line2 = "b a b";

    // discover the file set service
    ServiceManager serviceManager = applicationManager.startService("FileSetService");
    serviceManager.waitForStatus(true, 3, 60); // should be much faster, but justin case... wait 3x60sec
    URL serviceURL = serviceManager.getServiceURL();

    // write a file to the file set using the service
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL, "lines?path=nn.1").openConnection();
    try {
      connection.setDoOutput(true);
      connection.setRequestMethod("PUT");
      connection.getOutputStream().write(line1.getBytes(Charsets.UTF_8));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }

    // run word count over that file only
    Map<String, String> runtimeArguments = Maps.newHashMap();
    Map<String, String> inputArgs = Maps.newHashMap();
    FileSetArguments.setInputPaths(inputArgs, "nn.1");
    Map<String, String> outputArgs = Maps.newHashMap();
    FileSetArguments.setOutputPath(outputArgs, "out.1");
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "lines", inputArgs));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "counts", outputArgs));

    MapReduceManager mapReduceManager = applicationManager.startMapReduce("WordCount", runtimeArguments);
    mapReduceManager.waitForFinish(5, TimeUnit.MINUTES);

    // retrieve the counts through the service and verify
    Map<String, Integer> counts = Maps.newHashMap();
    connection = (HttpURLConnection) new URL(serviceURL, "counts?path=out.1/part-r-00000").openConnection();
    try {
      connection.setRequestMethod("GET");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
      readCounts(connection.getInputStream(), counts);
    } finally {
      connection.disconnect();
    }
    // "a b a" should yield "a":2, "b":1
    Assert.assertEquals(2, counts.size());
    Assert.assertEquals(new Integer(2), counts.get("a"));
    Assert.assertEquals(new Integer(1), counts.get("b"));

    // write a file to the file set using the dataset directly
    DataSetManager<FileSet> linesManager = getDataset("lines");
    OutputStream output = linesManager.get().getLocation("nn.2").getOutputStream();
    try {
      output.write(line2.getBytes(Charsets.UTF_8));
    } finally {
      output.close();
    }

    // run word count over both files
    FileSetArguments.setInputPath(inputArgs, "nn.1");
    FileSetArguments.addInputPath(inputArgs, "nn.2");
    FileSetArguments.setOutputPath(outputArgs, "out.2");
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "lines", inputArgs));
    runtimeArguments.putAll(RuntimeArguments.addScope(Scope.DATASET, "counts", outputArgs));

    mapReduceManager = applicationManager.startMapReduce("WordCount", runtimeArguments);
    mapReduceManager.waitForFinish(5, TimeUnit.MINUTES);

    // retrieve the counts through the dataset API and verify
    // write a file to the file set using the dataset directly
    DataSetManager<FileSet> countsManager = getDataset("counts");

    counts.clear();
    Location resultLocation = countsManager.get().getLocation("out.2");
    Assert.assertTrue(resultLocation.isDirectory());
    for (Location child : resultLocation.list()) {
      if (child.getName().startsWith("part-")) { // only read part files, no check sums or done files
        readCounts(child.getInputStream(), counts);
      }
    }
    // "a b a" and "b a b" should yield "a":3, "b":3
    Assert.assertEquals(2, counts.size());
    Assert.assertEquals(new Integer(3), counts.get("a"));
    Assert.assertEquals(new Integer(3), counts.get("b"));

    serviceManager.stop();
  }

  /**
   * Helper to read an input stream, line by line, and parse each line in the format word:count,
   * and add it as an entry to the map that is passed in.
   */
  private static void readCounts(InputStream inputStream, Map<String, Integer> counts) throws IOException {
    try {
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      while (true) {
        String line = reader.readLine();
        if (line == null || line.isEmpty()) {
          break;
        }
        String[] fields = line.split(":");
        Assert.assertEquals(2, fields.length);
        counts.put(fields[0], Integer.valueOf(fields[1]));
      }
    } finally {
      inputStream.close();
    }
  }

}
