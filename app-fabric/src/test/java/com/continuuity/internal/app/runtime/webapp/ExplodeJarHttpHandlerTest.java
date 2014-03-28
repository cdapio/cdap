package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.http.BasicHandlerContext;
import com.continuuity.http.HttpResponder;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.jboss.netty.handler.codec.http.HttpRequest;
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
    String servePath = jarHttpHandler.getServePath(request.getHeader("Host"), request.getUri());
    request.setUri(servePath);
    jarHttpHandler.serve(request, responder);
  }
}
