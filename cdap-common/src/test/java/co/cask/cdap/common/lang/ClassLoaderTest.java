/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.lang;

import co.cask.cdap.api.annotation.Beta;
import co.cask.cdap.api.annotation.Property;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationConfigurer;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.io.Locations;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.test.AppJarHelper;
import co.cask.cdap.internal.io.SchemaGenerator;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import org.apache.twill.api.ClassAcceptor;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.ApplicationBundler;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Unit test for ClassLoader.
 */
public class ClassLoaderTest {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  @Test
  public void testPackageFilter() throws ClassNotFoundException {
    ClassLoader classLoader = new PackageFilterClassLoader(getClass().getClassLoader(), new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return input.startsWith("co.cask.cdap.api.");
      }
    });

    // Load allowed class. It should gives the same class.
    Class<?> cls = classLoader.loadClass(Application.class.getName());
    Assert.assertSame(Application.class, cls);

    // Try to load forbidden class. It should fail.
    try {
      classLoader.loadClass(SchemaGenerator.class.getName());
      Assert.fail();
    } catch (ClassNotFoundException e) {
      // Expected
    }

    // Load resource. Should succeed.
    Assert.assertNotNull(classLoader.getResource("logback-test.xml"));

    // Load class resource that is in the allowed package. Should succeed.
    String resourceName = Application.class.getName().replace('.', '/') + ".class";
    URL url = classLoader.getResource(resourceName);
    Assert.assertNotNull(url);
    Assert.assertEquals(getClass().getClassLoader().getResource(resourceName), url);

    // Load class resource that is in allowed package. Should return null.
    resourceName = SchemaGenerator.class.getName().replace('.', '/') + ".class";
    Assert.assertNull(classLoader.getResource(resourceName));
  }

  @Test
  public void testCombineClassLoader() throws ClassNotFoundException {
    // Creates a CombineClassLoader with two delegates.
    // One allows "co.cask.cdap.api.app", the other allows "co.cask.cdap.api.annotation"
    ClassLoader parent = getClass().getClassLoader();
    ClassLoader classLoader = new CombineClassLoader(null, ImmutableList.of(
      new PackageFilterClassLoader(parent, Predicates.equalTo(Application.class.getPackage().getName())),
      new PackageFilterClassLoader(parent, Predicates.equalTo(Beta.class.getPackage().getName()))
    ));

    // Should be able to load classes from those two packages
    Assert.assertSame(ApplicationConfigurer.class, classLoader.loadClass(ApplicationConfigurer.class.getName()));
    Assert.assertSame(Property.class, classLoader.loadClass(Property.class.getName()));

    // For classes not in those packages would failed.
    try {
      classLoader.loadClass(Bytes.class.getName());
      Assert.fail();
    } catch (ClassNotFoundException e) {
      // Expected
    }
  }

  @Test
  public void testWeakReferenceClassLoader() throws Exception {
    // Creates a jar that has Application class in it.
    Location jar = AppJarHelper.createDeploymentJar(new LocalLocationFactory(TMP_FOLDER.newFolder()),
                                                    ClassLoaderTest.class);
    // Create a class loader that load from that jar.
    File unpackDir = TMP_FOLDER.newFolder();
    BundleJarUtil.unJar(jar, unpackDir);
    ClassLoader cl = new DirectoryClassLoader(unpackDir, null, "lib");

    // Wrap it with the WeakReference ClassLoader
    ClassLoader classLoader = new WeakReferenceDelegatorClassLoader(cl);

    // Load class from the wrapped ClassLoader, should succeed and should be loaded by the delegating ClassLoader.
    Class<?> cls = classLoader.loadClass(ClassLoaderTest.class.getName());
    Assert.assertSame(cl, cls.getClassLoader());
    Assert.assertSame(cl, Delegators.getDelegate(classLoader, ClassLoader.class));

    // There is no good way to test the GC of the weak reference referent since it depends on GC.
  }

  @Test
  public void testExtraClassPath() throws IOException, ClassNotFoundException {
    File tmpDir = TMP_FOLDER.newFolder();

    // Create two jars, one with guava, one with gson
    ApplicationBundler bundler = new ApplicationBundler(new ClassAcceptor());
    Location guavaJar = Locations.toLocation(new File(tmpDir, "guava.jar"));
    bundler.createBundle(guavaJar, ImmutableList.class);

    Location gsonJar = Locations.toLocation(new File(tmpDir, "gson.jar"));
    bundler.createBundle(gsonJar, Gson.class);

    // Unpack them
    File guavaDir = BundleJarUtil.unJar(guavaJar, TMP_FOLDER.newFolder());
    File gsonDir = BundleJarUtil.unJar(gsonJar, TMP_FOLDER.newFolder());

    // Create a DirectoryClassLoader using guava dir as the main directory, with the gson dir in the extra classpath
    String extraClassPath = gsonDir.getAbsolutePath() + File.pathSeparatorChar + gsonDir.getAbsolutePath() + "/lib/*";
    ClassLoader cl = new DirectoryClassLoader(guavaDir, extraClassPath, null, Arrays.asList("lib"));

    // Should be able to load both guava and gson class from the class loader
    cl.loadClass(ImmutableList.class.getName());
    cl.loadClass(Gson.class.getName());
  }

  @Test
  public void testDefinePackage() throws ClassNotFoundException {
    // This test is to test classes defined by the InterceptableClassLoader also has package being defined.
    // Create a classloader using urls from the current classloader and parent as the parent of the current classloader
    List<URL> urls = ClassLoaders.getClassLoaderURLs(getClass().getClassLoader(), new ArrayList<URL>());
    ClassLoader cl = new InterceptableClassLoader(urls.toArray(new URL[urls.size()]),
                                                  getClass().getClassLoader().getParent()) {
      @Override
      protected boolean needIntercept(String className) {
        // We intercept the ClassLoaderTest class (from local file) and all guava classes (from jar)
        return ClassLoaderTest.class.getName().equals(className) || className.startsWith("com.google.common.");
      }

      @Override
      public byte[] rewriteClass(String className, InputStream input) throws IOException {
        // We don't really rewrite class. Simply read the bytecode from the input stream.
        return ByteStreams.toByteArray(input);
      }
    };

    // Load a non-intercepted class. It should be defined by the intercepting classloader
    Class<?> cls = cl.loadClass(Application.class.getName());
    Assert.assertSame(cl, cls.getClassLoader());
    Assert.assertEquals(Application.class.getPackage().getName(), cls.getPackage().getName());;

    // Load an intercepted class that comes from local file system (non-jar).
    cls = cl.loadClass(ClassLoaderTest.class.getName());
    Assert.assertSame(cl, cls.getClassLoader());
    Assert.assertEquals(ClassLoaderTest.class.getPackage().getName(), cls.getPackage().getName());

    // Load an intercepted guava class that comes from jar file.
    cls = cl.loadClass(Function.class.getName());
    Assert.assertSame(cl, cls.getClassLoader());
    Package functionPackage = cls.getPackage();
    Assert.assertEquals(Function.class.getPackage().getName(), functionPackage.getName());

    // Load another intercepted guava class that comes from jar file with the same package as Function.
    cls = cl.loadClass(Supplier.class.getName());
    Assert.assertSame(cl, cls.getClassLoader());
    // The Supplier package should be the same as the Function package.
    Assert.assertSame(functionPackage, cls.getPackage());
  }
}
