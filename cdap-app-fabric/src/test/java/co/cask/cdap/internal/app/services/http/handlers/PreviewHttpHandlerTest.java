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
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.element.EntityType;
import co.cask.cdap.proto.id.EntityId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.cdap.proto.id.ParentedId;
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

  public class PreviewId extends EntityId implements NamespacedId, ParentedId<NamespaceId> {

    private final String namespace;
    private final String preview;

    public PreviewId(String namespace, String preview) {
      super(EntityType.APPLICATION);
      this.namespace = namespace;
      this.preview = preview;
    }

    @Override
    protected Iterable<String> toIdParts() {
      return null;
    }

    @Override
    public Id toId() {
      return null;
    }

    @Override
    public String getNamespace() {
      return namespace;
    }

    @Override
    public NamespaceId getParent() {
      return new NamespaceId(namespace);
    }

    public String getPreview() {
      return preview;
    }
  }

  @Test
  public void testPreviewEndPoints() throws Exception {
    HttpResponse response = doPost(getVersionedAPIPath("preview", "default"));
    String responseContent = EntityUtils.toString(response.getEntity());
    LOG.info(responseContent);
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    PreviewId id = GSON.fromJson(responseContent, PreviewId.class);

    String path = String.format("previews/%s/status", id.getPreview());
    response = doGet(getVersionedAPIPath(path, "default"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    responseContent = EntityUtils.toString(response.getEntity());
    LOG.info(responseContent);

    path = String.format("previews/%s/stages/%s", id.getPreview(), "CSV%20Parser");
    response = doGet(getVersionedAPIPath(path, "default"));
    Assert.assertEquals(200, response.getStatusLine().getStatusCode());
    responseContent = EntityUtils.toString(response.getEntity());
    LOG.info(responseContent);
  }
}
