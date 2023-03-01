/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.common.lang;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.twill.api.ClassAcceptor;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link ClassPathResources}.
 */
public class ClassPathResourcesTest {

  @Test
  public void testExcludeMultiReleaseJARVersionedBelow9() throws Exception {
    // Test that tracing dependencies is only done for dependencies matching the current JVM version or below for
    // multi-release JARs.
    // See https://docs.oracle.com/javase/10/docs/specs/jar/jar.html#multi-release-jar-files for details.
    Collection<String> result = new ArrayList<>();
    ClassAcceptor acceptor = ClassPathResources.createClassAcceptor(this.getClass().getClassLoader(), result);
    // Hardcode java specification version for test.
    System.setProperty("java.specification.version", "1.8");
    Assert.assertFalse(acceptor.accept("META-INF.versions.9.io.cdap.cdap.Test",
                                       new URL("file://META-INF/versions/9/io/cdap/cdap/Test.class"),
                                       new URL("file://")));
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testExcludeMultiReleaseJARVersionedDependenciesForPeriodVersions() throws Exception {
    // Test that tracing dependencies is only done for dependencies matching the current JVM version or below for
    // multi-release JARs.
    // See https://docs.oracle.com/javase/10/docs/specs/jar/jar.html#multi-release-jar-files for details.
    Collection<String> result = new ArrayList<>();
    ClassAcceptor acceptor = ClassPathResources.createClassAcceptor(this.getClass().getClassLoader(), result);
    // Hardcode java specification version for test.
    System.setProperty("java.specification.version", "1.9");
    Assert.assertFalse(acceptor.accept("META-INF.versions.11.io.cdap.cdap.Test",
                                       new URL("file://META-INF/versions/11/io/cdap/cdap/Test.class"),
                                       new URL("file://")));
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testExcludeMultiReleaseJARVersionedDependenciesForNewerVersions() throws Exception {
    // Test that tracing dependencies is only done for dependencies matching the current JVM version or below for
    // multi-release JARs.
    // See https://docs.oracle.com/javase/10/docs/specs/jar/jar.html#multi-release-jar-files for details.
    Collection<String> result = new ArrayList<>();
    ClassAcceptor acceptor = ClassPathResources.createClassAcceptor(this.getClass().getClassLoader(), result);
    // Hardcode java specification version for test.
    System.setProperty("java.specification.version", "11");
    Assert.assertFalse(acceptor.accept("META-INF.versions.15.io.cdap.cdap.Test",
                                       new URL("file://META-INF/versions/15/io/cdap/cdap/Test.class"),
                                       new URL("file://")));
    Assert.assertTrue(result.isEmpty());
  }

  @Test
  public void testIncludeMultiReleaseJARVersionedDependenciesForMatchingVersions() throws Exception {
    // Test that tracing dependencies is only done for dependencies matching the current JVM version or below for
    // multi-release JARs.
    // See https://docs.oracle.com/javase/10/docs/specs/jar/jar.html#multi-release-jar-files for details.
    Collection<String> result = new ArrayList<>();
    ClassAcceptor acceptor = ClassPathResources.createClassAcceptor(this.getClass().getClassLoader(), result);
    // Hardcode java specification version for test.
    System.setProperty("java.specification.version", "11");
    Assert.assertTrue(acceptor.accept("META-INF.versions.11.io.cdap.cdap.Test",
                                       new URL("file://META-INF/versions/11/io/cdap/cdap/Test.class"),
                                       new URL("file://")));
  }

  @Test
  public void testIncludeMultiReleaseJARVersionedDependenciesForPreviousVersions() throws Exception {
    // Test that tracing dependencies is only done for dependencies matching the current JVM version or below for
    // multi-release JARs.
    // See https://docs.oracle.com/javase/10/docs/specs/jar/jar.html#multi-release-jar-files for details.
    Collection<String> result = new ArrayList<>();
    ClassAcceptor acceptor = ClassPathResources.createClassAcceptor(this.getClass().getClassLoader(), result);
    // Hardcode java specification version for test.
    System.setProperty("java.specification.version", "11");
    Assert.assertTrue(acceptor.accept("META-INF.versions.9.io.cdap.cdap.Test",
                                       new URL("file://META-INF/versions/9/io/cdap/cdap/Test.class"),
                                       new URL("file://")));
  }
}
