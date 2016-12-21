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

package co.cask.cdap.internal.app.services.http.handlers;

import co.cask.cdap.common.security.ImpersonationInfo;
import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Type;
import java.net.URL;
import java.util.List;
import javax.annotation.Nullable;

/**
 *
 */
public class ImpersonationHttpHandlerTest extends AppFabricTestBase {
  private static final Gson GSON = new Gson();
  private static final Type IMPERSONATION_INFO_LIST_TYPE = new TypeToken<List<ImpersonationInfo>>() { }.getType();

  @Test
  public void test() throws Exception {
    Assert.assertTrue(list().isEmpty());

    ImpersonationInfo impersonationInfo = new ImpersonationInfo("somePrinc/host.net@REALM.NET", "hdfs:///tmp/keytab");
    put(impersonationInfo);
    List<ImpersonationInfo> list = list();
    Assert.assertEquals(1, list.size());
    Assert.assertEquals(impersonationInfo, list.get(0));

    Assert.assertEquals(impersonationInfo, get(impersonationInfo.getPrincipal()));

    delete(impersonationInfo.getPrincipal());
    Assert.assertTrue(list().isEmpty());
  }

  private URL getHandlerURL(@Nullable String queryParamString) throws Exception {
    String path = "/v3/impersonation/principals";
    if (queryParamString != null) {
      path += queryParamString;
    }
    return getEndPoint(path).toURL();
  }

  private List<ImpersonationInfo> list() throws Exception {
    HttpResponse execute = HttpRequests.execute(HttpRequest.get(getHandlerURL(null)).build());
    Assert.assertEquals(HttpResponseStatus.OK.code(), execute.getResponseCode());
    return GSON.fromJson(execute.getResponseBodyAsString(), IMPERSONATION_INFO_LIST_TYPE);
  }

  private ImpersonationInfo get(String principal) throws Exception {
    HttpResponse execute = HttpRequests.execute(HttpRequest.get(getHandlerURL("?principal=" + principal)).build());
    Assert.assertEquals(HttpResponseStatus.OK.code(), execute.getResponseCode());
    return GSON.fromJson(execute.getResponseBodyAsString(), ImpersonationInfo.class);
  }

  private void put(ImpersonationInfo impersonationInfo) throws Exception {
    HttpResponse execute =
      HttpRequests.execute(HttpRequest.put(getHandlerURL(null)).withBody(GSON.toJson(impersonationInfo)).build());
    Assert.assertEquals(HttpResponseStatus.OK.code(), execute.getResponseCode());
  }

  private void delete(String principal) throws Exception {
    HttpResponse execute = HttpRequests.execute(HttpRequest.delete(getHandlerURL("?principal=" + principal)).build());
    Assert.assertEquals(HttpResponseStatus.OK.code(), execute.getResponseCode());
  }
}
