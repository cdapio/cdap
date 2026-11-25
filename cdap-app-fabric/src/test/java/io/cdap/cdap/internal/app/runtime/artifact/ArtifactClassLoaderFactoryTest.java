/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.internal.app.runtime.artifact;

import io.cdap.cdap.api.artifact.CloseableClassLoader;
import io.cdap.cdap.app.program.ManifestFields;
import io.cdap.cdap.app.runtime.ProgramRuntimeProvider;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.internal.app.runtime.ProgramRuntimeProviderLoader;
import io.cdap.cdap.internal.app.runtime.artifact.app.inspection.InspectionApp;
import io.cdap.cdap.internal.app.runtime.artifact.app.plugin.PluginTestRunnable;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.EmptyClass;
import io.cdap.cdap.internal.app.runtime.artifact.plugin.Plugin1;
import io.cdap.cdap.proto.ProgramType;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.security.impersonation.DefaultImpersonator;
import io.cdap.cdap.security.impersonation.EntityImpersonator;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

/**
 * Unit tests for {@link ArtifactClassLoaderFactory}.
 */
public class ArtifactClassLoaderFactoryTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static Location createMinimalJar(LocalLocationFactory locationFactory,
      File jarFile, Class<?>... classes) throws IOException {
    return createMinimalJar(locationFactory, jarFile, null, classes);
  }

  private static Location createMinimalJar(LocalLocationFactory locationFactory,
      File jarFile, @Nullable String exportPackage, Class<?>... classes) throws IOException {
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    if (exportPackage != null) {
      manifest.getMainAttributes().put(ManifestFields.EXPORT_PACKAGE, exportPackage);
    }
    try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jarFile), manifest)) {
      byte[] buffer = new byte[4096];
      for (Class<?> cls : classes) {
        String entryName = cls.getName().replace('.', '/') + ".class";
        try (InputStream is = cls.getClassLoader().getResourceAsStream(entryName)) {
          Assert.assertNotNull(is);
          jos.putNextEntry(new JarEntry(entryName));
          int read;
          while ((read = is.read(buffer)) != -1) {
            jos.write(buffer, 0, read);
          }
          jos.closeEntry();
        }
      }
    }
    return locationFactory.create(jarFile.toURI());
  }

  @Test
  public void testSingleAndMultiLocationClassLoadersAndCloseCleanup() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("data").getAbsolutePath());

    File jarsDir = TEMP_FOLDER.newFolder("jars");
    LocalLocationFactory locationFactory = new LocalLocationFactory(jarsDir);
    Location firstJar = createMinimalJar(
        locationFactory, new File(jarsDir, "first.jar"),
        EmptyClass.class, PluginTestRunnable.class);
    Location secondJar = createMinimalJar(
        locationFactory, new File(jarsDir, "second.jar"),
        Plugin1.class.getPackage().getName(),
        Plugin1.class, Plugin1.P1Config.class, InspectionApp.class);

    EntityImpersonator entityImpersonator = new EntityImpersonator(
        NamespaceId.DEFAULT.artifact("art", "1.0.0"), new DefaultImpersonator(cConf, null));

    ArtifactClassLoaderFactory factory = new ArtifactClassLoaderFactory(cConf);

    try (CloseableClassLoader singleLoader =
        factory.createClassLoader(firstJar, entityImpersonator)) {
      Assert.assertNotNull(singleLoader.loadClass(EmptyClass.class.getName()));
    }

    try (CloseableClassLoader chainLoader = factory.createClassLoader(
        Arrays.asList(firstJar, secondJar).iterator(), entityImpersonator)) {
      Assert.assertNotNull(chainLoader.loadClass(EmptyClass.class.getName()));
      Assert.assertNotNull(chainLoader.loadClass(Plugin1.class.getName()));
    }

    Assert.assertThrows(IllegalArgumentException.class,
        () -> factory.createClassLoader(
            Collections.<Location>emptyList().iterator(), entityImpersonator));
  }

  @Test
  public void testCloseQuietlyWithThrowingSparkClassLoader() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("sparkData").getAbsolutePath());

    AtomicInteger sparkCloseCalls = new AtomicInteger();
    ClassLoader throwingSparkLoader = new URLClassLoader(new URL[0], getClass().getClassLoader()) {
      @Override
      public void close() throws IOException {
        sparkCloseCalls.incrementAndGet();
        throw new IOException("spark loader close failure");
      }
    };

    ProgramRuntimeProvider sparkProvider = Mockito.mock(ProgramRuntimeProvider.class);
    Mockito.when(sparkProvider.createProgramClassLoader(cConf, ProgramType.SPARK))
        .thenReturn(throwingSparkLoader);
    ProgramRuntimeProviderLoader providerLoader = Mockito.mock(ProgramRuntimeProviderLoader.class);
    Mockito.when(providerLoader.get(ProgramType.SPARK)).thenReturn(sparkProvider);

    ArtifactClassLoaderFactory factory = new ArtifactClassLoaderFactory(cConf, providerLoader);
    CloseableClassLoader loader = factory.createClassLoader(TEMP_FOLDER.newFolder("unpacked"));
    loader.close();
    Assert.assertEquals(1, sparkCloseCalls.get());
  }

  @Test
  public void testImpersonationRuntimeAndCheckedExceptionPropagation() throws Exception {
    CConfiguration cConf = CConfiguration.create();
    cConf.set(Constants.CFG_LOCAL_DATA_DIR, TEMP_FOLDER.newFolder("errData").getAbsolutePath());
    ArtifactClassLoaderFactory factory = new ArtifactClassLoaderFactory(cConf);

    File errJarsDir = TEMP_FOLDER.newFolder("errJars");
    LocalLocationFactory locationFactory = new LocalLocationFactory(errJarsDir);
    Location jar1 = createMinimalJar(
        locationFactory, new File(errJarsDir, "jar1.jar"), EmptyClass.class);
    Location jar2 = createMinimalJar(
        locationFactory, new File(errJarsDir, "jar2.jar"), Plugin1.class);

    EntityImpersonator impersonator = Mockito.mock(EntityImpersonator.class);
    IllegalStateException expectedRuntime = new IllegalStateException("impersonation runtime");
    Mockito.when(impersonator.impersonate(Mockito.any())).thenThrow(expectedRuntime);

    try {
      factory.createClassLoader(jar1, impersonator);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    try {
      factory.createClassLoader(Arrays.asList(jar1, jar2).iterator(), impersonator);
      Assert.fail("Expected IllegalStateException");
    } catch (RuntimeException actual) {
      Assert.assertSame(expectedRuntime, actual);
    }

    Mockito.reset(impersonator);
    IOException ioException = new IOException("impersonation io");
    Mockito.when(impersonator.impersonate(Mockito.any())).thenThrow(ioException);

    RuntimeException singleWrapped = Assert.assertThrows(
        RuntimeException.class, () -> factory.createClassLoader(jar1, impersonator));
    Assert.assertSame(ioException, singleWrapped.getCause());

    RuntimeException multiWrapped = Assert.assertThrows(RuntimeException.class,
        () -> factory.createClassLoader(Arrays.asList(jar1, jar2).iterator(), impersonator));
    Assert.assertSame(ioException, multiWrapped.getCause());
  }
}
