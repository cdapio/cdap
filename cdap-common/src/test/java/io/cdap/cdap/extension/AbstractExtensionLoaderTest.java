/*
 * Copyright © 2024 Cask Data, Inc.
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

package io.cdap.cdap.extension;

import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link AbstractExtensionLoader}.
 */
public class AbstractExtensionLoaderTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  /**
   * Test extension contract.
   */
  public interface SampleExtension {
    Set<String> supportedTypes();
  }

  /**
   * Concrete extension implementation packaged into a module JAR for testing.
   */
  public static final class AlphaSampleExtension implements SampleExtension {
    @Override
    public Set<String> supportedTypes() {
      return Collections.singleton("alpha");
    }
  }

  private static final class SampleExtensionLoader
      extends AbstractExtensionLoader<String, SampleExtension> {

    SampleExtensionLoader(String extDirs) {
      super(extDirs);
    }

    @Override
    protected Set<String> getSupportedTypesForProvider(SampleExtension extension) {
      return extension.supportedTypes();
    }

    static Set<String> packagesFor(Set<String> resources) {
      return createPackageSets(resources);
    }
  }

  @Test
  public void testLoadExtensionsFromModuleJarAndPackageSets() throws Exception {
    File extRoot = TEMP_FOLDER.newFolder();
    File moduleDir = new File(extRoot, "module-alpha");
    Assert.assertTrue(moduleDir.mkdirs());
    File jarFile = new File(moduleDir, "alpha-ext.jar");

    String implResource = AlphaSampleExtension.class.getName().replace('.', '/') + ".class";
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile));
         InputStream classBytes = getClass().getClassLoader().getResourceAsStream(implResource)) {
      Assert.assertNotNull(classBytes);
      jos.putNextEntry(new JarEntry(implResource));
      ByteStreams.copy(classBytes, jos);
      jos.closeEntry();

      String serviceEntry = "META-INF/services/" + SampleExtension.class.getName();
      jos.putNextEntry(new JarEntry(serviceEntry));
      jos.write((AlphaSampleExtension.class.getName() + "\n").getBytes(StandardCharsets.UTF_8));
      jos.closeEntry();
    }

    SampleExtensionLoader loader = new SampleExtensionLoader(
        extRoot.getAbsolutePath() + ";/nonexistent/ext/dir");

    SampleExtension alpha = loader.get("alpha");
    Assert.assertNotNull(alpha);
    Assert.assertEquals(Collections.singleton("alpha"), alpha.supportedTypes());
    Assert.assertNull(loader.get("unknown"));

    Map<String, SampleExtension> all = loader.getAll();
    Assert.assertEquals(1, all.size());
    Assert.assertSame(all, loader.getAll());

    Set<String> pkgs = SampleExtensionLoader.packagesFor(new HashSet<>(Arrays.asList(
        "io/cdap/ext/Foo.class",
        "TopLevel.class",
        "META-INF/MANIFEST.MF")));
    Assert.assertEquals(Collections.singleton("io.cdap.ext"), pkgs);
  }
}
