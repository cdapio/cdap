/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.common.conf;

import co.cask.cdap.api.common.Bytes;
import com.google.common.io.Closeables;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Map;

/**
 * Testing CConfiguration.
 */
public class CConfigurationTest {

  private static final String DEPRECATED_PROPERTY_VALUE = "Value of deprecated property";

  @Test
  public void testConfiguration() throws Exception {
    // first test empty config object
    CConfiguration conf = CConfiguration.create();
    String a = conf.get("conf.test.A");
    String b = conf.get("conf.test.B");
    Assert.assertNull(a);
    Assert.assertNull(b);
    // load some defaults and make sure they work
    conf.addResource("test-default.xml");
    a = conf.get("conf.test.A");
    b = conf.get("conf.test.B");
    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
    Assert.assertEquals("A", a);
    Assert.assertEquals("B", b);
    // override one of the defaults and verify
    conf.addResource("test-override.xml");
    a = conf.get("conf.test.A");
    b = conf.get("conf.test.B");
    Assert.assertNotNull(a);
    Assert.assertNotNull(b);
    Assert.assertEquals("A", a);
    Assert.assertEquals("B+", b);
  }

  @Test
  public void testAddedConfiguration() throws Exception {
    CConfiguration conf = CConfiguration.create();
    conf.addResource("test-default.xml");
    conf.set("conf.test.addedA", "AddedA");
    conf.set("conf.test.addedB", "AddedB");
    conf.set("conf.test.A", "A+");
    Assert.assertNotNull(conf.get("conf.test.A"));
    Assert.assertNotNull(conf.get("conf.test.B"));
    Assert.assertNotNull(conf.get("conf.test.addedA"));
    Assert.assertNotNull(conf.get("conf.test.addedB"));
    Assert.assertEquals("A+", conf.get("conf.test.A"));
    Assert.assertEquals("AddedA", conf.get("conf.test.addedA"));
    Assert.assertEquals("AddedB", conf.get("conf.test.addedB"));
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
      Assert.fail("Expected getInt() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(1, conf.getInt("test.property.int"));

    try {
      conf.getLong("missing.property");
      Assert.fail("Expected getLong() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(1L, conf.getLong("test.property.long"));

    try {
      conf.getLongBytes("missing.property");
      Assert.fail("Expected getLongBytes() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(1024L, conf.getLongBytes("test.property.longbytes"));

    try {
      conf.getFloat("missing.property");
      Assert.fail("Expected getFloat() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(1.1f, conf.getFloat("test.property.float"), 0.01f);

    try {
      conf.getBoolean("missing.property");
      Assert.fail("Expected getBoolean() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(true, conf.getBoolean("test.property.boolean"));

    try {
      conf.getEnum("missing.property", TestEnum.class);
      Assert.fail("Expected getEnum() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(TestEnum.FIRST, conf.getEnum("test.property.enum", TestEnum.class));

    try {
      conf.getPattern("missing.property");
      Assert.fail("Expected getPattern() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(testRegex, conf.getPattern("test.property.pattern").pattern());

    try {
      conf.getRange("missing.property");
      Assert.fail("Expected getRange() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
  }

  @Test
  public void testDeprecatedConfigProperties() throws Exception {
    CConfiguration conf = CConfiguration.create();
    Map<String, String[]> deprecated = conf.getDeprecatedProps();

    if (!deprecated.isEmpty()) {
      Map.Entry<String, String[]> property = deprecated.entrySet().iterator().next();

      // Test a deprecated property on loading from resources
      InputStream resource = new ByteArrayInputStream(Bytes.toBytes(
        String.format("<configuration><property><name>%s</name><value>%s</value></property></configuration>",
                      property.getKey(), DEPRECATED_PROPERTY_VALUE)));
      conf.addResource(resource);
      conf.reloadConfiguration();
      // Validate new properties, which should be used instead of deprecated
      for (String newProperty : property.getValue()) {
        Assert.assertEquals(DEPRECATED_PROPERTY_VALUE, conf.get(newProperty));
      }

      // Clear the Config before next test
      conf.clear();

      // Test a deprecated property on set from the code
      conf.set(property.getKey(), DEPRECATED_PROPERTY_VALUE);
      // Validate new properties, which should be used instead of deprecated
      for (String newProperty : property.getValue()) {
        Assert.assertEquals(DEPRECATED_PROPERTY_VALUE, conf.get(newProperty));
      }

      // Close the InputStream
      Closeables.closeQuietly(resource);
    }
  }

  private enum TestEnum { FIRST }
}
