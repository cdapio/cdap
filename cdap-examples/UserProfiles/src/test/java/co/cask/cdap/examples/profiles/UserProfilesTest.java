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

package co.cask.cdap.examples.profiles;

import co.cask.cdap.api.dataset.table.Get;
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.FlowManager;
import co.cask.cdap.test.RuntimeMetrics;
import co.cask.cdap.test.RuntimeStats;
import co.cask.cdap.test.ServiceManager;
import co.cask.cdap.test.StreamWriter;
import co.cask.cdap.test.TestBase;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 * Tests the UserProfiles example app.
 */
public class UserProfilesTest extends TestBase {

  @Test
  public void testUserProfiles() throws Exception {

    // deploy the app
    ApplicationManager applicationManager = deployApplication(UserProfiles.class);

    // run the service and the flow
    FlowManager flowManager = applicationManager.startFlow("ActivityFlow");

    ServiceManager serviceManager = applicationManager.startService("UserProfileService");
    serviceManager.waitForStatus(true, 3, 60); // should be much faster, but justin case... wait 3x60sec
    URL serviceURL = serviceManager.getServiceURL();

    // create a user through the service
    String userJson = new Gson().toJson(ImmutableMap.of("id", "1234", "name", "joe", "email", "joe@bla.ck"));
    HttpURLConnection connection = (HttpURLConnection) new URL(serviceURL, "profiles/1234").openConnection();
    try {
      connection.setDoOutput(true);
      connection.setRequestMethod("PUT");
      connection.getOutputStream().write(userJson.getBytes(Charsets.UTF_8));
      Assert.assertEquals(HttpURLConnection.HTTP_CREATED, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }

    // read the user through the dataset
    DataSetManager<Table> tableManager = getDataset("profiles");
    Row row = tableManager.get().get(new Get("1234"));
    Assert.assertEquals("1234", row.getString("id"));
    Assert.assertEquals("joe", row.getString("name"));
    Assert.assertEquals("joe@bla.ck", row.getString("email"));
    Assert.assertNull(row.getLong("login"));
    Assert.assertNull(row.getLong("active"));

    // update email address through service
    connection = (HttpURLConnection) new URL(serviceURL, "profiles/1234/email").openConnection();
    try {
      connection.setDoOutput(true);
      connection.setRequestMethod("PUT");
      connection.getOutputStream().write("joe@black.com".getBytes(Charsets.UTF_8));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }

    // verify the updated email address
    tableManager.flush();
    row = tableManager.get().get(new Get("1234"));
    Assert.assertEquals("1234", row.getString("id"));
    Assert.assertEquals("joe", row.getString("name"));
    Assert.assertEquals("joe@black.com", row.getString("email"));
    Assert.assertNull(row.getLong("login"));
    Assert.assertNull(row.getLong("active"));

    // send a login event
    long loginTime = System.currentTimeMillis();
    connection = (HttpURLConnection) new URL(serviceURL, "profiles/1234/lastLogin").openConnection();
    try {
      connection.setDoOutput(true);
      connection.setRequestMethod("PUT");
      connection.getOutputStream().write(Long.toString(loginTime).getBytes(Charsets.UTF_8));
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }

    // verify the login time through the dataset
    tableManager.flush();
    row = tableManager.get().get(new Get("1234"));
    Assert.assertEquals("1234", row.getString("id"));
    Assert.assertEquals("joe", row.getString("name"));
    Assert.assertEquals("joe@black.com", row.getString("email"));
    Assert.assertEquals(new Long(loginTime), row.getLong("login"));
    Assert.assertNull(row.getLong("active"));

    // send an event to the stream
    long activeTime = System.currentTimeMillis();
    StreamWriter streamWriter = applicationManager.getStreamWriter("events");
    streamWriter.send(new Gson().toJson(new Event(activeTime, "1234", "/some/path")));

    try {
      // Wait for the last Flowlet processing 1 events, or at most 5 seconds
      RuntimeMetrics metrics = RuntimeStats.getFlowletMetrics("UserProfiles", "ActivityFlow", "updater");
      metrics.waitForProcessed(1, 5, TimeUnit.SECONDS);
    } finally {
      flowManager.stop();
      Assert.assertFalse(flowManager.isRunning());
    }

    // verify the last active time for the user
    tableManager.flush();
    row = tableManager.get().get(new Get("1234"));
    Assert.assertEquals("1234", row.getString("id"));
    Assert.assertEquals("joe", row.getString("name"));
    Assert.assertEquals("joe@black.com", row.getString("email"));
    Assert.assertEquals(new Long(loginTime), row.getLong("login"));
    Assert.assertEquals(new Long(activeTime), row.getLong("active"));

    // delete the user
    connection = (HttpURLConnection) new URL(serviceURL, "profiles/1234").openConnection();
    try {
      connection.setRequestMethod("DELETE");
      Assert.assertEquals(HttpURLConnection.HTTP_OK, connection.getResponseCode());
    } finally {
      connection.disconnect();
    }

    // verify the user is gone
    tableManager.flush();
    row = tableManager.get().get(new Get("1234"));
    Assert.assertTrue(row.isEmpty());

    // stop the service and the flow
    serviceManager.stop();
  }

}
