/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.metadata;

import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.internal.AppFabricTestHelper;
import co.cask.cdap.internal.MockResponder;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.http.HttpResponder;
import com.google.inject.Injector;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests for {@link MetadataHttpHandler}.
 */
public class MetadataHttpHandlerTest {

  private static CConfiguration cConf;
  private static MetadataAdmin metadataAdmin;
  private static MetadataHttpHandler httpHandler;
  private static HttpRequest httpRequest;
  private static HttpResponder httpResponder;

  @BeforeClass
  public static void setup() throws Exception {
    cConf = CConfiguration.create();
    final Injector injector = AppFabricTestHelper.getInjector(cConf);
    metadataAdmin = injector.getInstance(MetadataAdmin.class);
    httpHandler = new MetadataHttpHandler(metadataAdmin);
    httpRequest = new DefaultHttpRequest(new HttpVersion("HTTP/1.1"), new HttpMethod("GET"), "testURL");
    httpResponder = new MockResponder();
  }

  @Test
  public void testInvalidSearch() throws Exception {
    try {
      httpHandler.searchMetadata(httpRequest, httpResponder, NamespaceId.DEFAULT.getNamespace(), null, null,
                                 null, 0, 2147483647, 0, null, false, null);
    } catch (Exception e) {
      Assert.assertTrue(e.getClass().equals(BadRequestException.class));
      return;
    }
    Assert.fail("Metadata search without query should fail");
  }
}
