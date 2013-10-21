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

    Assert.assertEquals("webapp/127.0.0.1:20000/index.html",
                        servePathGenerator.getServePath("127.0.0.1:20000", "index.html"));

    Assert.assertEquals("webapp/default/1.txt",
                        servePathGenerator.getServePath("127.0.0.1:80", "/1.txt"));

    Assert.assertEquals("webapp/default/index.html",
                        servePathGenerator.getServePath("127.0.0.1:30000", "index.html"));


    servePathGenerator = new ServePathGenerator(Constants.Webapp.WEBAPP_DIR + "/", fileExists);
    Assert.assertEquals("webapp/www.abc.com:80/data/data.txt",
                        servePathGenerator.getServePath("www.abc.com", "data/data.txt"));

    Assert.assertEquals("webapp/www.abc.com:80/data/data.txt",
                        servePathGenerator.getServePath("www.abc.com:80", "data/data.txt"));

    Assert.assertEquals("webapp/default/data/data.txt",
                        servePathGenerator.getServePath("www.abc.com:30000", "data/data.txt"));


    Assert.assertEquals("webapp/default/data/data.txt",
                        servePathGenerator.getServePath("www.xyz.com", "data/data.txt"));

    Assert.assertEquals("webapp/default/data/data.txt",
                        servePathGenerator.getServePath("www.xyz.com:80", "data/data.txt"));

    Assert.assertEquals("webapp/default:20000/data/data.txt",
                        servePathGenerator.getServePath("www.xyz.com:20000", "data/data.txt"));
  }
}
