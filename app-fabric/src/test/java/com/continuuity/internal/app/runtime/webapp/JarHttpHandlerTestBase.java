package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.http.HttpResponder;
import com.continuuity.http.InternalHttpResponder;
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
