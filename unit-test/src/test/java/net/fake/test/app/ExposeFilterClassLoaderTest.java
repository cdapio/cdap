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

package net.fake.test.app;

import co.cask.cdap.api.annotation.ExposeClass;
import co.cask.cdap.common.lang.jar.BundleJarUtil;
import co.cask.cdap.common.lang.jar.ExposeFilterClassLoader;
import co.cask.cdap.common.utils.DirUtils;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Set;

/**
 * Testing the {@link co.cask.cdap.common.lang.jar.ExposeFilterClassLoader}
 * we are using the Purchase app for testing, if purchase app is updated, update the resources directory with
 * the new version.
 *
 * we are doing this, because if we get a JarFinder.getJar of a class, it already exists in the classpath and the
 * bootstrap classloader which is the parent of the ExposeFilterClassLoader will be able to it,
 * thereby we are not testing the {@link co.cask.cdap.common.lang.jar.ExposeFilterClassLoader}
 *
 * If we pass in "null" for the parent classloader for the {@link ExposeFilterClassLoader} , though we are able to load
 * the class, we are not able to load the annotations. so we had to take this approach.
 */
public class ExposeFilterClassLoaderTest {

  @Test
  public void testExposedClass() throws ClassNotFoundException, IOException, URISyntaxException {

    Set<String> annotations = Sets.newHashSet();
    annotations.add(ExposeClass.class.getName());
    Predicate<String> annotationPredicate = Predicates.in(annotations);
    String jarPath = TestBundleJarApp.class.getClassLoader().getResource("purchase.jar").toURI().getPath();
    LocationFactory lf = new LocalLocationFactory();
    File dsFile = Files.createTempDir();
    try {
      BundleJarUtil.unpackProgramJar(lf.create(jarPath), dsFile);
      ClassLoader dsClassLoader = new ExposeFilterClassLoader(dsFile, Thread.currentThread().getContextClassLoader(),
                                                              annotationPredicate);
      dsClassLoader.loadClass("co.cask.cdap.examples.purchase.PurchaseHistory");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      DirUtils.deleteDirectoryContents(dsFile);
    }
  }

  @Test(expected = ClassNotFoundException.class)
  public void testUnExposedClass() throws ClassNotFoundException, IOException, URISyntaxException {

    Set<String> annotations = Sets.newHashSet();
    annotations.add(ExposeClass.class.getName());
    Predicate<String> annotationPredicate = Predicates.in(annotations);
    String jarPath = TestBundleJarApp.class.getClassLoader().getResource("purchase.jar").toURI().getPath();
    LocationFactory lf = new LocalLocationFactory();
    File dsFile = Files.createTempDir();
    try {
      BundleJarUtil.unpackProgramJar(lf.create(jarPath), dsFile);
      ClassLoader dsClassLoader = new ExposeFilterClassLoader(dsFile, Thread.currentThread().getContextClassLoader(),
                                                              annotationPredicate);
      dsClassLoader.loadClass("co.cask.cdap.examples.purchase.PurchaseFlow");
    } catch (IOException e) {
      throw Throwables.propagate(e);
    } finally {
      DirUtils.deleteDirectoryContents(dsFile);
    }
  }

}
