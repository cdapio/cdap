/*
 * Copyright 2014 Cask, Inc.
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
import co.cask.http.InternalHttpResponder;
import org.apache.commons.io.IOUtils;
import org.jboss.netty.handler.codec.http.DefaultHttpRequest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;

/**
 * Base class for jar http handler tests.
 */
public abstract class JarHttpHandlerTestBase {
  protected abstract void serve(HttpRequest request, HttpResponder responder);

  @Test
  public void testServe() throws Exception {
    InternalHttpResponder responder = new InternalHttpResponder();
    serve(createRequest("/netlens/1.txt", "www.continuuity.net:20000"), responder);

    Assert.assertEquals(HttpResponseStatus.OK.getCode(), responder.getResponse().getStatusCode());
    Assert.assertEquals("1 line default",
                        IOUtils.toString(responder.getResponse().getInputSupplier().getInput()).trim());
  }

  @Test
  public void testServe404() throws Exception {
    InternalHttpResponder responder = new InternalHttpResponder();
    serve(createRequest("/geo/nofile.txt", "www.abc.com:80"), responder);

    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), responder.getResponse().getStatusCode());
  }

  @Test
  public void testServeDir() throws Exception {
    InternalHttpResponder responder = new InternalHttpResponder();
    serve(createRequest("/geo/data", "www.abc.com:80"), responder);

    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.getCode(), responder.getResponse().getStatusCode());
  }

  @Test
  public void testServeParent() throws Exception {
    InternalHttpResponder responder = new InternalHttpResponder();
    serve(createRequest("/geo/../../../../../../", "www.abc.com:80"), responder);

    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), responder.getResponse().getStatusCode());
  }

  private HttpRequest createRequest(String uri, String host) {
    DefaultHttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
    request.setHeader("Host", host);
    return request;
  }
}
