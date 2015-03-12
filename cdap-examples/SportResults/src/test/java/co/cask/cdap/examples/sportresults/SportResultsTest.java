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

package co.cask.cdap.examples.sportresults;

import co.cask.cdap.api.dataset.lib.PartitionKey;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.MapReduceManager;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import jline.internal.InputStreamReader;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Tests the functioning and completeness of the Sports app.
 */
public class SportResultsTest extends TestBase {

  private static final String FANTASY_2014 =
    "2014/1/3,My Team,Your Team,24,17\n" +
      "2014/3/5,My Team,Other Team,17,16\n";
  private static final String FANTASY_2015 =
    "2015/3/10,Your Team,My Team,32,12\n" +
      "2014/8/12,Other Team,Your Team,24,14\n";
  private static final String CRITTERS_2014 =
    "2015/3/10,Red Falcons,Blue Bonnets,28,17\n" +
      "2014/8/12,Green Berets,Red Falcons,23,8\n";

  @Test
  public void testPartitionedCounting() throws Exception {

    // deploy the application and start the upload service
    ApplicationManager appManager = deployApplication(SportResults.class);
    ServiceManager serviceManager = appManager.startService("UploadService");
    serviceManager.waitForStatus(true);

    // upload a few dummy results
    URL url = serviceManager.getServiceURL();
    uploadResults(url, "fantasy", 2014, FANTASY_2014);
    uploadResults(url, "fantasy", 2015, FANTASY_2015);
    uploadResults(url, "critters", 2014, CRITTERS_2014);

    // start a map/reduce that counts all seasons for the fantasy league
    MapReduceManager mrManager = appManager.startMapReduce("ScoreCounter", ImmutableMap.of("league", "fantasy"));
    mrManager.waitForFinish(5, TimeUnit.MINUTES); // should be much faster, though

    // validate the output by reading directly from the file set
    DataSetManager<PartitionedFileSet> dataSetManager = getDataset("totals");
    PartitionedFileSet totals = dataSetManager.get();
    String path = totals.getPartition(PartitionKey.builder().addStringField("league", "fantasy").build());
    Assert.assertNotNull(path);
    Location location = totals.getEmbeddedFileSet().getLocation(path);

    // find the part file that has the actual results
    Assert.assertTrue(location.isDirectory());
    for (Location file : location.list()) {
      if (file.getName().startsWith("part")) {
        location = file;
      }
    }
    BufferedReader reader = new BufferedReader(new InputStreamReader(location.getInputStream()));

    // validate each line
    Map<String, String[]> expected = ImmutableMap.of(
      "My Team", new String[] { "My Team", "2", "0", "1", "53", "65" },
      "Your Team", new String[] { "Your Team", "1", "0", "2", "63", "60" },
      "Other Team", new String[] { "Other Team", "1", "0", "1", "40", "31" });
    while (true) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      String[] fields = line.split(",");
      Assert.assertArrayEquals(expected.get(fields[0]), fields);
    }

    // verify using SQL
    // query with SQL
    Connection connection = getQueryClient();
    ResultSet results = connection
      .prepareStatement("SELECT wins, ties, losses, scored, conceded " +
                          "FROM dataset_totals WHERE team = 'My Team' AND league = 'fantasy'")
      .executeQuery();

    // should return only one row, with correct time fields
    Assert.assertTrue(results.next());
    Assert.assertEquals(2, results.getInt(1));
    Assert.assertEquals(0, results.getInt(2));
    Assert.assertEquals(1, results.getInt(3));
    Assert.assertEquals(53, results.getInt(4));
    Assert.assertEquals(65, results.getInt(5));
    Assert.assertFalse(results.next());
  }

  private void uploadResults(URL url, String league, int season, String content) throws Exception {
    HttpURLConnection connection = (HttpURLConnection)
      new URL(url, String.format("leagues/%s/seasons/%d", league, season)).openConnection();
    try {
      connection.setDoOutput(true);
      connection.setRequestMethod("PUT");
      connection.getOutputStream().write(content.getBytes(Charsets.UTF_8));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }
  }

  // write a file to the file set using the service



}
