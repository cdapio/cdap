/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.webapp;

import co.cask.http.HttpResponder;
import co.cask.http.internal.BasicHandlerContext;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.net.URL;

/**
 * ExplodeJarHttpHandler Test.
 */
public class ExplodeJarHttpHandlerTest extends JarHttpHandlerTestBase {
  private static ExplodeJarHttpHandler jarHttpHandler;

  @BeforeClass
  public static void init() throws Exception {
    URL jarUrl = ExplodeJarHttpHandlerTest.class.getResource("/CountRandomWebapp-localhost.jar");
    Assert.assertNotNull(jarUrl);

    jarHttpHandler = new ExplodeJarHttpHandler(new LocalLocationFactory().create(jarUrl.toURI()));
    jarHttpHandler.init(new BasicHandlerContext(null));
  }

  @AfterClass
  public static void destroy() throws Exception {
    jarHttpHandler.destroy(new BasicHandlerContext(null));
  }

  @Override
  protected void serve(HttpRequest request, HttpResponder responder) {
    String servePath = jarHttpHandler.getServePath(request.headers().get("Host"), request.uri());
    request.setUri(servePath);
    jarHttpHandler.serve(request, responder);
  }
}
