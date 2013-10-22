package com.continuuity.internal.app.runtime.webapp;

import com.continuuity.common.utils.Networks;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.InputStream;
import java.util.Set;

/**
 * Tests WebappProgramRunner.
 */
public class WebappProgramRunnerTest {
  @Test
  public void testGetServingHostNames() throws Exception {
    InputStream jarInputStream = getClass().getResourceAsStream("/CountRandomWebapp-localhost.jar");
    Assert.assertNotNull(jarInputStream);

    Set<String> expected = ImmutableSet.of(Networks.normalizeWebappDiscoveryName("127.0.0.1:20000/geo"),
                                           Networks.normalizeWebappDiscoveryName("127.0.0.1:20000/netlens"),
                                           Networks.normalizeWebappDiscoveryName("127.0.0.1:20000"),
                                           Networks.normalizeWebappDiscoveryName("default/netlens"),
                                           Networks.normalizeWebappDiscoveryName("www.abc.com:80/geo"));

    Set<String> hostnames = WebappProgramRunner.getServingHostNames(jarInputStream);
    Assert.assertEquals(expected, hostnames);
  }


  @Test
  public void testGetNoServingHostNames() throws Exception {
    InputStream jarInputStream = getClass().getResourceAsStream("/test_explode.jar");
    Assert.assertNotNull(jarInputStream);

    Set<String> hostnames = WebappProgramRunner.getServingHostNames(jarInputStream);
    Assert.assertTrue(hostnames.isEmpty());
  }
}
