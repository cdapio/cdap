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

package co.cask.cdap.data2.datafabric.dataset.service;

import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for {@link UnderlyingSystemNamespaceHandler}
 */
public class UnderlyingSystemNamespaceHandlerTest extends DatasetServiceTestBase {

  @Test
  public void test() throws IOException {
    Assert.assertEquals(200, createNamespace("myspace").getResponseCode());
    Assert.assertEquals(500, createNamespace("myspace").getResponseCode());
    Assert.assertEquals(200, deleteNamespace("myspace").getResponseCode());
  }

  private HttpResponse createNamespace(String namespaceId) throws IOException {
    HttpRequest request = HttpRequest.put(getUnderlyingNamespaceAdminUrl(namespaceId, "create")).build();
    return HttpRequests.execute(request);
  }

  private HttpResponse deleteNamespace(String namespaceId) throws IOException {
    HttpRequest request = HttpRequest.delete(getUnderlyingNamespaceAdminUrl(namespaceId, "delete")).build();
    return HttpRequests.execute(request);
  }
}
