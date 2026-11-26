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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.xml.sax.SAXParseException;

/**
 * Testing CConfiguration.
 */
public class CConfigurationTest {

  private static final String DEPRECATED_PROPERTY_VALUE = "Value of deprecated property";

  @Rule
  public final TemporaryFolder tmpFolder = new TemporaryFolder();

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
  public void testDoctypeXxeRejectedFromInputStream() throws Exception {
    File secretFile = tmpFolder.newFile("secret.txt");
    Files.writeString(secretFile.toPath(), "TOP_SECRET_VALUE", StandardCharsets.UTF_8);

    String xxeXml = "<?xml version=\"1.0\"?>"
        + "<!DOCTYPE configuration [<!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">]>"
        + "<configuration><property><name>xxe.prop</name><value>&xxe;</value></property>"
        + "</configuration>";

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(new ByteArrayInputStream(xxeXml.getBytes(StandardCharsets.UTF_8)));
    try {
      cConf.get("xxe.prop");
      Assert.fail("Expected RuntimeException when loading XML with DOCTYPE declaration");
    } catch (RuntimeException e) {
      Assert.assertTrue("Expected SAXParseException cause, got: " + e.getCause(),
          e.getCause() instanceof SAXParseException);
    }
  }

  @Test
  public void testDoctypeXxeRejectedFromUrl() throws Exception {
    File secretFile = tmpFolder.newFile("secret-url.txt");
    Files.writeString(secretFile.toPath(), "TOP_SECRET_URL_VALUE", StandardCharsets.UTF_8);

    File xmlFile = tmpFolder.newFile("xxe-site.xml");
    String xxeXml = "<?xml version=\"1.0\"?>"
        + "<!DOCTYPE configuration [<!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">]>"
        + "<configuration><property><name>xxe.url.prop</name><value>&xxe;</value></property>"
        + "</configuration>";
    Files.writeString(xmlFile.toPath(), xxeXml, StandardCharsets.UTF_8);

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(xmlFile.toURI().toURL());
    try {
      cConf.get("xxe.url.prop");
      Assert.fail("Expected RuntimeException when loading XML URL with DOCTYPE declaration");
    } catch (RuntimeException e) {
      Assert.assertTrue("Expected SAXParseException cause, got: " + e.getCause(),
          e.getCause() instanceof SAXParseException);
    }
  }

  @Test
  public void testParameterEntityXxeRejected() throws Exception {
    File dtdFile = tmpFolder.newFile("external.dtd");
    Files.writeString(dtdFile.toPath(), "<!ENTITY param 'LEAKED'>", StandardCharsets.UTF_8);

    String xml = "<?xml version=\"1.0\"?>"
        + "<!DOCTYPE configuration [<!ENTITY % pe SYSTEM \"" + dtdFile.toURI() + "\"> %pe;]>"
        + "<configuration><property><name>pe.prop</name><value>v</value></property>"
        + "</configuration>";

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    try {
      cConf.get("pe.prop");
      Assert.fail("Expected RuntimeException for external parameter entity DOCTYPE");
    } catch (RuntimeException e) {
      Assert.assertTrue("Expected SAXParseException cause, got: " + e.getCause(),
          e.getCause() instanceof SAXParseException);
    }
  }

  @Test
  public void testXincludeDisabledFromInputStream() throws Exception {
    File secretFile = tmpFolder.newFile("xinclude-secret.txt");
    Files.writeString(secretFile.toPath(), "XINCLUDE_SECRET_TEXT", StandardCharsets.UTF_8);

    File includedXml = tmpFolder.newFile("included-conf.xml");
    Files.writeString(includedXml.toPath(),
        "<configuration><property><name>included.key</name><value>INCLUDED_VAL</value></property>"
            + "</configuration>",
        StandardCharsets.UTF_8);

    String xml = "<?xml version=\"1.0\"?>"
        + "<configuration xmlns:xi=\"http://www.w3.org/2001/XInclude\">"
        + "<xi:include href=\"" + includedXml.toURI() + "\"/>"
        + "<property><name>xinclude.text</name><value>"
        + "<xi:include href=\"" + secretFile.toURI() + "\" parse=\"text\"/>"
        + "</value></property>"
        + "<property><name>normal.key</name><value>normal.val</value></property>"
        + "</configuration>";

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    Assert.assertNull(cConf.get("xinclude.text"));
    Assert.assertNull(cConf.get("included.key"));
    Assert.assertEquals("normal.val", cConf.get("normal.key"));
  }

  @Test
  public void testXincludeDisabledFromUrl() throws Exception {
    File secretFile = tmpFolder.newFile("xinclude-url-secret.txt");
    Files.writeString(secretFile.toPath(), "XINCLUDE_URL_SECRET", StandardCharsets.UTF_8);

    File xmlFile = tmpFolder.newFile("xinclude-site.xml");
    String xml = "<?xml version=\"1.0\"?>"
        + "<configuration xmlns:xi=\"http://www.w3.org/2001/XInclude\">"
        + "<property><name>control.key</name><value>loaded</value></property>"
        + "<property><name>xinclude.url</name><value>"
        + "<xi:include href=\"" + secretFile.toURI() + "\" parse=\"text\"/>"
        + "</value></property>"
        + "</configuration>";
    Files.writeString(xmlFile.toPath(), xml, StandardCharsets.UTF_8);

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(xmlFile.toURI().toURL());
    Assert.assertEquals("loaded", cConf.get("control.key"));
    Assert.assertNull(cConf.get("xinclude.url"));
  }

  @Test
  public void testNormalXmlWithCdataCommentsAndWriteRoundTrip() throws Exception {
    String xml = "<?xml version=\"1.0\"?>"
        + "<configuration>"
        + "<!-- leading comment -->"
        + "<property><name>cdata.key</name><value><![CDATA[a < b & c > d]]></value></property>"
        + "<property><name>plain.key</name><value>plain &amp; simple</value></property>"
        + "</configuration>";

    CConfiguration cConf = CConfiguration.create();
    cConf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals("a < b & c > d", cConf.get("cdata.key"));
    Assert.assertEquals("plain & simple", cConf.get("plain.key"));

    StringWriter writer = new StringWriter();
    cConf.writeXml(writer);
    CConfiguration reloaded = CConfiguration.create(
        new ByteArrayInputStream(writer.toString().getBytes(StandardCharsets.UTF_8)));
    Assert.assertEquals("a < b & c > d", reloaded.get("cdata.key"));
    Assert.assertEquals("plain & simple", reloaded.get("plain.key"));
  }

  private enum TestEnum {FIRST}
}
