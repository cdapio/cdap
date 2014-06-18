package com.continuuity.common.conf;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
    assertEquals("A", a);
    assertEquals("B", b);
    // override one of the defaults and verify
    conf.addResource("test-override.xml");
    a = conf.get("continuuity.test.A");
    b = conf.get("continuuity.test.B");
    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
    assertEquals("A", a);
    assertEquals("B+", b);
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
    assertEquals("A+", conf.get("continuuity.test.A"));
    assertEquals("AddedA", conf.get("continuuity.test.addedA"));
    assertEquals("AddedB", conf.get("continuuity.test.addedB"));
  }

  @Test
  public void testMissingConfigProperties() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.setInt("test.property.int", 1);
    conf.setLong("test.property.long", 1L);
    conf.set("test.property.longbytes", "1k");
    conf.setFloat("test.property.float", 1.1f);
    conf.set("test.property.boolean", "true");
    conf.set("test.property.enum", TestEnum.FIRST.name());
    String testRegex = ".*";
    conf.set("test.property.pattern", testRegex);


    try {
      conf.getInt("missing.property");
      fail("Expected getInt() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    assertEquals(1, conf.getInt("test.property.int"));

    try {
      conf.getLong("missing.property");
      fail("Expected getLong() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    assertEquals(1L, conf.getLong("test.property.long"));

    try {
      conf.getLongBytes("missing.property");
      fail("Expected getLongBytes() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    assertEquals(1024L, conf.getLongBytes("test.property.longbytes"));

    try {
      conf.getFloat("missing.property");
      fail("Expected getFloat() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    assertEquals(1.1f, conf.getFloat("test.property.float"), 0.01f);

    try {
      conf.getBoolean("missing.property");
      fail("Expected getBoolean() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    assertEquals(true, conf.getBoolean("test.property.boolean"));

    try {
      conf.getEnum("missing.property", TestEnum.class);
      fail("Expected getEnum() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    assertEquals(TestEnum.FIRST, conf.getEnum("test.property.enum", TestEnum.class));

    try {
      conf.getPattern("missing.property");
      fail("Expected getPattern() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    assertEquals(testRegex, conf.getPattern("test.property.pattern").pattern());

    try {
      conf.getRange("missing.property");
      fail("Expected getRange() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
  }

  private enum TestEnum { FIRST };
}
