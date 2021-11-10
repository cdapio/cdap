/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.support.handlers;

import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.services.http.AppFabricTestBase;
import io.cdap.common.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Test;

/**
 * Monitor handler tests.
 */
public class SupportBundleHttpHandlerTest extends AppFabricTestBase {

  @Test
  public void testCreateSupportBundle() throws Exception {
    createNamespace("default");
    String path =
      String.format("%s/support/bundle", Constants.Gateway.API_VERSION_3);
    HttpResponse response = doPost(path);
    Assert.assertEquals(HttpResponseStatus.CREATED.code(), response.getResponseCode());

    Assert.assertNotNull(response.getResponseBodyAsString());
  }

  @Test
  public void testCreateSupportBundleWithValidNamespace() throws Exception {
    createNamespace("default");
    String path =
        String.format("%s/support/bundle?namespaceId=default", Constants.Gateway.API_VERSION_3);
    HttpResponse response = doPost(path);
    Assert.assertEquals(HttpResponseStatus.CREATED.code(), response.getResponseCode());

    Assert.assertNotNull(response.getResponseBodyAsString());
  }
}
