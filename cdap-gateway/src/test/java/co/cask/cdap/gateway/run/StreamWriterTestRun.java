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

package co.cask.cdap.gateway.run;

import co.cask.cdap.api.data.stream.StreamWriter;
import co.cask.cdap.gateway.GatewayFastTestsSuite;
import co.cask.cdap.gateway.GatewayTestBase;
import co.cask.cdap.gateway.apps.AppWritingtoStream;
import co.cask.cdap.proto.ProgramRunStatus;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * {@link StreamWriter} Tests.
 */
public class StreamWriterTestRun extends GatewayTestBase {

  @Test
  public void testStreamWrites() throws Exception {
    HttpResponse response = GatewayFastTestsSuite.deploy(AppWritingtoStream.class, TEMP_FOLDER.newFolder());
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    //Start Flow
    response = GatewayFastTestsSuite.doPost(String.format("/v3/namespaces/default/apps/%s/flows/%s/start",
                                                          AppWritingtoStream.APPNAME,
                                                          AppWritingtoStream.FLOW), null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    //Start Worker
    response = GatewayFastTestsSuite.doPost(String.format("/v3/namespaces/default/apps/%s/workers/%s/start",
                                                          AppWritingtoStream.APPNAME,
                                                          AppWritingtoStream.WORKER), null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());
    //Start Service
    response = GatewayFastTestsSuite.doPost(String.format("/v3/namespaces/default/apps/%s/services/%s/start",
                                                          AppWritingtoStream.APPNAME,
                                                          AppWritingtoStream.SERVICE), null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    waitForProgramRuns("flows", AppWritingtoStream.APPNAME, AppWritingtoStream.FLOW, ProgramRunStatus.RUNNING, 1);
    waitForProgramRuns("services", AppWritingtoStream.APPNAME, AppWritingtoStream.SERVICE, ProgramRunStatus.RUNNING, 1);

    checkCount(AppWritingtoStream.VALUE);
    checkHeader("Event", "1");

    //Stop Flow
    response = GatewayFastTestsSuite.doPost(String.format("/v3/namespaces/default/apps/%s/flows/%s/stop",
                                                          AppWritingtoStream.APPNAME,
                                                          AppWritingtoStream.FLOW), null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    //Stop Service
    response = GatewayFastTestsSuite.doPost(String.format("/v3/namespaces/default/apps/%s/services/%s/stop",
                                                          AppWritingtoStream.APPNAME,
                                                          AppWritingtoStream.SERVICE), null);
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());

    waitForProgramRuns("flows", AppWritingtoStream.APPNAME, AppWritingtoStream.FLOW, ProgramRunStatus.KILLED, 1);
    waitForProgramRuns("workers", AppWritingtoStream.APPNAME, AppWritingtoStream.WORKER, ProgramRunStatus.COMPLETED, 1);
    waitForProgramRuns("services", AppWritingtoStream.APPNAME, AppWritingtoStream.SERVICE, ProgramRunStatus.KILLED, 1);

    response = GatewayFastTestsSuite.doDelete(String.format("/v3/namespaces/default/apps/%s",
                                                            AppWritingtoStream.APPNAME));
    Assert.assertEquals(HttpResponseStatus.OK.code(), response.getStatusLine().getStatusCode());


  }

  private void checkCount(int expected) throws Exception {
    int trials = 0;
    while (trials++ < 5) {
      HttpResponse response = GatewayFastTestsSuite.doGet(
        String.format("/v3/namespaces/default/apps/%s/services/%s/methods/%s",
                      AppWritingtoStream.APPNAME,
                      AppWritingtoStream.SERVICE,
                      AppWritingtoStream.ENDPOINT));
      if (response.getStatusLine().getStatusCode() == HttpResponseStatus.OK.code()) {
        String count = EntityUtils.toString(response.getEntity());
        if (expected == Integer.valueOf(count)) {
          break;
        }
      }
      TimeUnit.MILLISECONDS.sleep(250);
    }
    Assert.assertTrue(trials < 5);
  }

  private void checkHeader(String key, String expected) throws Exception {
    int trials = 0;
    while (trials++ < 5) {
      HttpResponse response = GatewayFastTestsSuite.doGet(
        String.format("/v3/namespaces/default/apps/%s/services/%s/methods/headers/%s",
          AppWritingtoStream.APPNAME,
          AppWritingtoStream.SERVICE,
          key));
      if (response.getStatusLine().getStatusCode() == HttpResponseStatus.OK.code()) {
        String val = EntityUtils.toString(response.getEntity());
        if (expected.equals(val)) {
          break;
        }
      }
      TimeUnit.MILLISECONDS.sleep(250);
    }
    Assert.assertTrue(trials < 5);
  }
}
