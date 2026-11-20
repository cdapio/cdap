/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.common.guice;

import com.google.inject.ConfigurationException;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Verifies that Guice error formatting and stack traces succeed on Java 11 (class file version 55)
 * CDAP classes without failing in Guice 4.0's shaded ASM5 {@code LineNumbers}.
 */
public class GuiceErrorMessageTest {

  private interface UnboundDependency {
  }

  private static final class UnboundConsumer {
    private final UnboundDependency dependency;

    @Inject
    UnboundConsumer(UnboundDependency dependency) {
      this.dependency = dependency;
    }
  }

  @Test
  public void testConfigurationExceptionFormattingOnJava11Class() throws Exception {
    String resourcePath = UnboundConsumer.class.getName().replace('.', '/') + ".class";
    try (InputStream in = UnboundConsumer.class.getClassLoader().getResourceAsStream(resourcePath);
         DataInputStream dataIn = new DataInputStream(in)) {
      Assert.assertNotNull("Expected class resource for " + resourcePath, in);
      Assert.assertEquals(0xCAFEBABE, dataIn.readInt());
      dataIn.readUnsignedShort(); // minor version
      int majorVersion = dataIn.readUnsignedShort();
      Assert.assertEquals("Expected Java 11 (v55) bytecode", 55, majorVersion);
    }

    Injector injector = Guice.createInjector();
    try {
      injector.getInstance(UnboundConsumer.class);
      Assert.fail("Expected ConfigurationException for unbound dependency");
    } catch (ConfigurationException expected) {
      String message = expected.getMessage();
      Assert.assertNotNull(message);
      Assert.assertTrue("Unexpected Guice message: " + message,
          message.contains("No implementation for " + UnboundDependency.class.getName()));

      StringWriter sw = new StringWriter();
      try (PrintWriter pw = new PrintWriter(sw)) {
        expected.printStackTrace(pw);
      }
      Assert.assertTrue(sw.toString().contains(ConfigurationException.class.getName()));
    }
  }
}
