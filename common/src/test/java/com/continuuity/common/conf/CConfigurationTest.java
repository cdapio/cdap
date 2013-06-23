package com.continuuity.common.conf;

import org.junit.Assert;
import org.junit.Test;

/**
 * Testing CConfiguration.
 */
public class CConfigurationTest {

  @Test
  public void testConfiguration() throws Exception {
    // first test empty config object
    CConfiguration conf = CConfiguration.create();
    String a = conf.get("continuuity.test.A");
    String b = conf.get("continuuity.test.B");
    Assert.assertNull(a);
    Assert.assertNull(b);
    // load some defaults and make sure they work
    conf.addResource("test-default.xml");
    a = conf.get("continuuity.test.A");
    b = conf.get("continuuity.test.B");
    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
    Assert.assertEquals("A", a);
    Assert.assertEquals("B", b);
    // override one of the defaults and verify
    conf.addResource("test-override.xml");
    a = conf.get("continuuity.test.A");
    b = conf.get("continuuity.test.B");
    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
    Assert.assertEquals("A", a);
    Assert.assertEquals("B+", b);
  }

  @Test
  public void testAddedConfiguration() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.addResource("test-default.xml");
    conf.set("continuuity.test.addedA", "AddedA");
    conf.set("continuuity.test.addedB", "AddedB");
    conf.set("continuuity.test.A", "A+");
    Assert.assertNotNull(conf.get("continuuity.test.A"));
    Assert.assertNotNull(conf.get("continuuity.test.B"));
    Assert.assertNotNull(conf.get("continuuity.test.addedA"));
    Assert.assertNotNull(conf.get("continuuity.test.addedB"));
    Assert.assertEquals("A+", conf.get("continuuity.test.A"));
    Assert.assertEquals("AddedA", conf.get("continuuity.test.addedA"));
    Assert.assertEquals("AddedB", conf.get("continuuity.test.addedB"));
  }

}
