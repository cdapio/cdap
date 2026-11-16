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

package io.cdap.cdap.common.conf;

import com.google.common.io.Closeables;
import io.cdap.cdap.api.common.Bytes;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.SAXException;

/**
 * Testing CConfiguration.
 */
public class CConfigurationTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

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
    conf.setDouble("test.property.double", 1.2d);
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
      conf.getDouble("missing.property");
      Assert.fail("Expected getDouble() to throw NullPointerException");
    } catch (NullPointerException e) {
      // expected
    }
    Assert.assertEquals(1.2d, conf.getDouble("test.property.double"), 0.01d);

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
          String.format(
              "<configuration><property><name>%s</name><value>%s</value></property></configuration>",
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

  @Test
  public void testActivation() {
    CConfiguration cConf = CConfiguration.create();
    cConf.addResource("test-default.xml");

    Assert.assertEquals(2, cConf.getInt("conf.test.activate"));
  }

  @Test
  public void testActivationWithCopy() {
    CConfiguration cConf = CConfiguration.create();
    cConf.addResource("test-default.xml");

    cConf = CConfiguration.copy(cConf);

    Assert.assertEquals(2, cConf.getInt("conf.test.activate"));
  }

  @Test
  public void testActivationWithOverride() {
    CConfiguration cConf = CConfiguration.create();
    cConf.addResource("test-default.xml");

    Assert.assertEquals(2, cConf.getInt("conf.test.activate"));

    cConf.addResource("test-override.xml");
    Assert.assertEquals(3, cConf.getInt("conf.test.activate"));
  }

  @Test
  public void testActivationCombine() throws IOException {
    CConfiguration cConf = CConfiguration.create();
    cConf.addResource("test-default.xml");
    cConf.addResource("test-override.xml");

    StringWriter writer = new StringWriter();
    cConf.writeXml(writer);

    cConf = CConfiguration.create(
        new ByteArrayInputStream(writer.toString().getBytes(StandardCharsets.UTF_8)));

    cConf.addResource("test-override.xml");
    Assert.assertEquals(3, cConf.getInt("conf.test.activate"));
  }

  @Test
  public void testRejectsExternalGeneralEntity() throws Exception {
    File secretFile = TEMP_FOLDER.newFile("secret-general.txt");
    Files.write(secretFile.toPath(), "SENSITIVE_TOKEN_VALUE".getBytes(StandardCharsets.UTF_8));
    String xml =
        "<?xml version=\"1.0\"?>\n"
            + "<!DOCTYPE configuration [\n"
            + "  <!ENTITY xxe SYSTEM \""
            + secretFile.toURI()
            + "\">\n"
            + "]>\n"
            + "<configuration>\n"
            + "  <property><name>xxe.key</name><value>&xxe;</value></property>\n"
            + "</configuration>";

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    try {
      cConf.get("xxe.key");
      Assert.fail("Expected RuntimeException when parsing XML with DOCTYPE and external entity");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof SAXException);
      Assert.assertFalse(e.getMessage().contains("SENSITIVE_TOKEN_VALUE"));
    }

    File xmlFile = TEMP_FOLDER.newFile("xxe-config.xml");
    Files.write(xmlFile.toPath(), xml.getBytes(StandardCharsets.UTF_8));
    CConfiguration urlConf = CConfiguration.create();
    urlConf.addResource(xmlFile.toURI().toURL());
    try {
      urlConf.get("xxe.key");
      Assert.fail("Expected RuntimeException when parsing URL resource with DOCTYPE");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof SAXException);
      Assert.assertFalse(e.getMessage().contains("SENSITIVE_TOKEN_VALUE"));
    }
  }

  @Test
  public void testRejectsExternalParameterEntityAndDtd() throws Exception {
    File dtdFile = TEMP_FOLDER.newFile("external.dtd");
    Files.write(
        dtdFile.toPath(), "<!ENTITY extParam \"LEAKED\">".getBytes(StandardCharsets.UTF_8));
    String paramEntityXml =
        "<?xml version=\"1.0\"?>\n"
            + "<!DOCTYPE configuration [\n"
            + "  <!ENTITY % pe SYSTEM \""
            + dtdFile.toURI()
            + "\">\n"
            + "  %pe;\n"
            + "]>\n"
            + "<configuration>\n"
            + "  <property><name>pe.key</name><value>val</value></property>\n"
            + "</configuration>";

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(new ByteArrayInputStream(paramEntityXml.getBytes(StandardCharsets.UTF_8)));
    try {
      cConf.get("pe.key");
      Assert.fail("Expected RuntimeException when parsing XML with external parameter entity");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof SAXException);
    }

    String externalDtdXml =
        "<?xml version=\"1.0\"?>\n"
            + "<!DOCTYPE configuration SYSTEM \""
            + dtdFile.toURI()
            + "\">\n"
            + "<configuration>\n"
            + "  <property><name>dtd.key</name><value>val</value></property>\n"
            + "</configuration>";

    CConfiguration dtdConf = CConfiguration.create();
    dtdConf.addResource(new ByteArrayInputStream(externalDtdXml.getBytes(StandardCharsets.UTF_8)));
    try {
      dtdConf.get("dtd.key");
      Assert.fail("Expected RuntimeException when parsing XML with external DTD");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof SAXException);
    }
  }

  @Test
  public void testRejectsXinclude() throws Exception {
    File secretFile = TEMP_FOLDER.newFile("secret-xinclude.txt");
    Files.write(secretFile.toPath(), "XINCLUDE_SECRET".getBytes(StandardCharsets.UTF_8));
    String xincludeInValueXml =
        "<?xml version=\"1.0\"?>\n"
            + "<configuration>\n"
            + "  <property>\n"
            + "    <name>xinclude.key</name>\n"
            + "    <value><xi:include xmlns:xi=\"http://www.w3.org/2001/XInclude\" href=\""
            + secretFile.toURI()
            + "\" parse=\"text\"/></value>\n"
            + "  </property>\n"
            + "</configuration>";

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(
        new ByteArrayInputStream(xincludeInValueXml.getBytes(StandardCharsets.UTF_8)));
    try {
      cConf.get("xinclude.key");
      Assert.fail("Expected RuntimeException when parsing XML with xi:include in value");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof SAXException);
      Assert.assertFalse(e.getMessage().contains("XINCLUDE_SECRET"));
    }

    String xincludeInRootXml =
        "<?xml version=\"1.0\"?>\n"
            + "<configuration>\n"
            + "  <include xmlns=\"http://www.w3.org/2001/XInclude\" href=\""
            + secretFile.toURI()
            + "\"/>\n"
            + "</configuration>";
    CConfiguration rootConf = CConfiguration.create();
    rootConf.addResource(
        new ByteArrayInputStream(xincludeInRootXml.getBytes(StandardCharsets.UTF_8)));
    try {
      rootConf.get("any.key");
      Assert.fail("Expected RuntimeException when parsing XML with XInclude element");
    } catch (RuntimeException e) {
      Assert.assertTrue(e.getCause() instanceof SAXException);
      Assert.assertFalse(e.getMessage().contains("XINCLUDE_SECRET"));
    }
  }

  @Test
  public void testStandardPredefinedEntitiesStillParse() {
    String validXml =
        "<?xml version=\"1.0\"?>\n"
            + "<configuration>\n"
            + "  <property><name>valid.key</name><value>a &amp; b &lt; c &gt; d</value></property>\n"
            + "</configuration>";
    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(new ByteArrayInputStream(validXml.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals("a & b < c > d", cConf.get("valid.key"));
  }

  private enum TestEnum {FIRST}
}
