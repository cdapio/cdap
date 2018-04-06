/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.cdap.proto.ops.DashboardProgramRunRecord;
import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.util.List;

/**
 * Tests for {@link OperationsDashboardHttpHandler}
 */
public class OperationsDashboardHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final String BASE_PATH = Constants.Gateway.API_VERSION_3;
  private static final Type DASHBOARD_DETAIL_TYPE = new TypeToken<List<DashboardProgramRunRecord>>() {
  }.getType();

  @Test
  public void testDashboardDetail() throws Exception {
    HttpResponse response = doGet(BASE_PATH + "/dashboard?start=1000&duration=1440&namespace=mao&namespace=default");
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String content = new String(ByteStreams.toByteArray(response.getEntity().getContent()), Charsets.UTF_8);
    List<DashboardProgramRunRecord> dashboardDetail = GSON.fromJson(content, DASHBOARD_DETAIL_TYPE);
    Assert.assertEquals(60, dashboardDetail.size());
  }
}
