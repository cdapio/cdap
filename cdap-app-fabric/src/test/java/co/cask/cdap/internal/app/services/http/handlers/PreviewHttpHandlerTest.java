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

import co.cask.cdap.internal.app.services.http.AppFabricTestBase;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class PreviewHttpHandlerTest extends AppFabricTestBase {

  private static final Logger LOG = LoggerFactory.getLogger(PreviewHttpHandlerTest.class);
  @Test
  public void testPreviewEndPoints() throws Exception {
    HttpResponse response = doPost(getVersionedAPIPath("preview", "default"));
    String previewId = EntityUtils.toString(response.getEntity());
    LOG.info(previewId);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());

    String path = String.format("previews/%s/status", previewId);
    response = doGet(getVersionedAPIPath(path, "default"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    String responseContent = EntityUtils.toString(response.getEntity());
    LOG.info(responseContent);

    path = String.format("previews/%s/stages/%s", previewId, "CSV%20Parser");
    response = doGet(getVersionedAPIPath(path, "default"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    responseContent = EntityUtils.toString(response.getEntity());
    LOG.info(responseContent);
  }
}
