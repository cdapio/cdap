package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.archive.JarResources;
import com.continuuity.common.conf.Constants;
import com.continuuity.weave.filesystem.LocalLocationFactory;
import com.google.common.base.Predicate;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.net.URL;

/**
 * Tests ServePathGenerator.
 */
public class ServePathGeneratorTest {
  @Test
  public void testGetServePath() throws Exception {
    URL jarUrl = getClass().getResource("/CountRandomWebapp-localhost.jar");
    Assert.assertNotNull(jarUrl);

    final JarResources jarResources = new JarResources(new LocalLocationFactory().create(jarUrl.toURI()));
    Predicate<String> fileExists = new Predicate<String>() {
      @Override
      public boolean apply(@Nullable String file) {
        return file != null && jarResources.getResource(file) != null;
      }
    };

    ServePathGenerator servePathGenerator = new ServePathGenerator(Constants.Webapp.WEBAPP_DIR, fileExists);

    Assert.assertEquals("webapp/127.0.0.1:20000/netlens/src/index.html",
                        servePathGenerator.getServePath("127.0.0.1:20000", "/netlens"));

    Assert.assertEquals("webapp/127.0.0.1:20000/netlens/src/index.html",
                        servePathGenerator.getServePath("127.0.0.1:20000", "/netlens/index.html"));

    Assert.assertEquals("webapp/127.0.0.1:20000/netlens/src/index.html",
                        servePathGenerator.getServePath("127.0.0.1:20000", "/netlens/"));

    Assert.assertEquals("webapp/127.0.0.1:20000/netlens/src/index.html",
                        servePathGenerator.getServePath("127.0.0.1:20000", "/netlens/"));

    Assert.assertEquals("webapp/default/src/index.html",
                        servePathGenerator.getServePath("127.0.0.1:30000", "/"));

    Assert.assertEquals("webapp/default/src/index.html",
                        servePathGenerator.getServePath("127.0.0.1:30000", "/index.html"));

    Assert.assertEquals("webapp/127.0.0.1:20000/src/netlens/2.txt",
                        servePathGenerator.getServePath("127.0.0.1:20000", "netlens/2.txt"));

    Assert.assertEquals("webapp/default/netlens/src/1.txt",
                        servePathGenerator.getServePath("127.0.0.1:80", "/netlens/1.txt"));

    Assert.assertEquals("webapp/default/netlens/src/data/data.txt",
                        servePathGenerator.getServePath("127.0.0.1:30000", "/netlens/data/data.txt"));


    servePathGenerator = new ServePathGenerator(Constants.Webapp.WEBAPP_DIR + "/", fileExists);
    Assert.assertEquals("webapp/www.abc.com:80/geo/src/data/data.txt",
                        servePathGenerator.getServePath("www.abc.com", "/geo/data/data.txt"));

    Assert.assertEquals("webapp/www.abc.com:80/geo/src/data/data.txt",
                        servePathGenerator.getServePath("www.abc.com:80", "/geo/data/data.txt"));

    Assert.assertNull(servePathGenerator.getServePath("www.abc.com:30000", "/geo/data/data.txt"));

    Assert.assertEquals("webapp/default/netlens/src/data/data.txt",
                        servePathGenerator.getServePath("www.abc.com:30000", "/netlens/data/data.txt"));
  }
}
