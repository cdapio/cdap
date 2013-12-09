package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.http.core.BasicHandlerContext;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.net.URL;

/**
 * ExplodeJarHttpHandler Test.
 */
public class ExplodeJarHttpHandlerTest extends JarHttpHandlerTestBase {
  @BeforeClass
  public static void init() throws Exception {
    URL jarUrl = IntactJarHttpHandlerTest.class.getResource("/CountRandomWebapp-localhost.jar");
    Assert.assertNotNull(jarUrl);

    jarHttpHandler = new IntactJarHttpHandler(new LocalLocationFactory().create(jarUrl.toURI()));
    jarHttpHandler.init(new BasicHandlerContext(null));
  }

  @AfterClass
  public static void destroy() throws Exception {
    jarHttpHandler.destroy(new BasicHandlerContext(null));
  }
}
