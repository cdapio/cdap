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
import co.cask.cdap.internal.io.SchemaGenerator;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.net.URL;

/**
 * Unit test for ClassLoader.
 */
public class ClassLoaderTest {

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
}
