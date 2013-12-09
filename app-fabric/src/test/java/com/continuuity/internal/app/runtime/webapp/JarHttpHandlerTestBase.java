package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.http.core.InternalHttpResponder;
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
  protected static IntactJarHttpHandler jarHttpHandler;

  @Test
  public void testServe() throws Exception {
    InternalHttpResponder responder = new InternalHttpResponder();
    jarHttpHandler.serve(createRequest("webapp/default:20000/netlens/src/1.txt"), responder);

    Assert.assertEquals(HttpResponseStatus.OK.getCode(), responder.getResponse().getStatusCode());
    Assert.assertEquals("1 line default",
                        IOUtils.toString(responder.getResponse().getInputSupplier().getInput()).trim());
  }

  @Test
  public void testServe404() throws Exception {
    InternalHttpResponder responder = new InternalHttpResponder();
    jarHttpHandler.serve(createRequest("webapp/www.abc.com:80/geo/src/nofile.txt"), responder);

    Assert.assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), responder.getResponse().getStatusCode());
  }

  @Test
  public void testServeDir() throws Exception {
    InternalHttpResponder responder = new InternalHttpResponder();
    jarHttpHandler.serve(createRequest("webapp/www.abc.com:80/geo/src/"), responder);

    Assert.assertEquals(HttpResponseStatus.FORBIDDEN.getCode(), responder.getResponse().getStatusCode());
  }

  private HttpRequest createRequest(String uri) {
    return new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
  }

}
