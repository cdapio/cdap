/*
 * Copyright 2014 Cask, Inc.
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
package co.cask.cdap.common.lang.jar;

import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;

/**
 *
 */
public class JarResourcesTest {

  @ClassRule
  public static TemporaryFolder tmpFolder = new TemporaryFolder();

  @Test
  public void testGetResource() throws IOException {
    File jarFile = createJar(tmpFolder.newFile());
    Location jarLocation = new LocalLocationFactory(jarFile.getParentFile()).create(jarFile.toURI());

    String classPath = JarResourcesTest.class.getName().replace('.', '/') + ".class";

    JarResources jarResources = new JarResources(jarLocation);
    Assert.assertFalse(jarResources.contains("test"));
    Assert.assertTrue(jarResources.contains("logback-test.xml"));
    Assert.assertTrue(jarResources.contains(classPath));

    Assert.assertArrayEquals(
      ByteStreams.toByteArray(getClass().getClassLoader().getResourceAsStream("logback-test.xml")),
      jarResources.getResource("logback-test.xml")
    );

    Assert.assertArrayEquals(
      ByteStreams.toByteArray(getClass().getClassLoader().getResourceAsStream(classPath)),
      jarResources.getResource(classPath)
    );

    Assert.assertArrayEquals(
      ByteStreams.toByteArray(getClass().getClassLoader().getResourceAsStream("logback-test.xml")),
      ByteStreams.toByteArray(jarResources.getResourceAsStream("logback-test.xml"))
    );

    Assert.assertArrayEquals(
      ByteStreams.toByteArray(getClass().getClassLoader().getResourceAsStream(classPath)),
      ByteStreams.toByteArray(jarResources.getResourceAsStream(classPath))
    );
  }

  private File createJar(File target) throws IOException {
    JarOutputStream jarOutput = new JarOutputStream(new FileOutputStream(target));

    try {
      jarOutput.putNextEntry(new JarEntry("logback-test.xml"));
      ByteStreams.copy(getClass().getClassLoader().getResourceAsStream("logback-test.xml"), jarOutput);

      String classPath = JarResourcesTest.class.getName().replace('.', '/') + ".class";
      jarOutput.putNextEntry(new JarEntry(classPath));
      ByteStreams.copy(getClass().getClassLoader().getResourceAsStream(classPath), jarOutput);

      return target;
    } finally {
      jarOutput.close();
    }
  }
}
