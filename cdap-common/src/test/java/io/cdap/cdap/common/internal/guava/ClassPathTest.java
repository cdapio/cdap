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

package io.cdap.cdap.common.internal.guava;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Unit tests for {@link ClassPath}.
 */
public class ClassPathTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testClassInfoSimpleNameForTopLevelInnerLocalAndAnonymousClasses() throws Exception {
    URL baseUrl = TEMP_FOLDER.newFolder().toURI().toURL();
    ClassLoader cl = ClassPathTest.class.getClassLoader();

    ClassPath.ClassInfo topLevel = new ClassPath.ClassInfo(
        baseUrl, "io/cdap/cdap/SampleTopLevel.class", cl);
    Assert.assertEquals("io.cdap.cdap", topLevel.getPackageName());
    Assert.assertEquals("SampleTopLevel", topLevel.getSimpleName());
    Assert.assertEquals("io.cdap.cdap.SampleTopLevel", topLevel.getName());

    ClassPath.ClassInfo defaultPackage = new ClassPath.ClassInfo(
        baseUrl, "DefaultPkgClass.class", cl);
    Assert.assertEquals("", defaultPackage.getPackageName());
    Assert.assertEquals("DefaultPkgClass", defaultPackage.getSimpleName());

    ClassPath.ClassInfo memberInner = new ClassPath.ClassInfo(
        baseUrl, "io/cdap/cdap/Outer$NamedInner.class", cl);
    Assert.assertEquals("NamedInner", memberInner.getSimpleName());

    ClassPath.ClassInfo singleDigitAnon = new ClassPath.ClassInfo(
        baseUrl, "io/cdap/cdap/Outer$1.class", cl);
    Assert.assertEquals("", singleDigitAnon.getSimpleName());

    ClassPath.ClassInfo multiDigitAnon = new ClassPath.ClassInfo(
        baseUrl, "io/cdap/cdap/Outer$12.class", cl);
    Assert.assertEquals("", multiDigitAnon.getSimpleName());

    ClassPath.ClassInfo localClass = new ClassPath.ClassInfo(
        baseUrl, "io/cdap/cdap/Outer$1LocalHelper.class", cl);
    Assert.assertEquals("LocalHelper", localClass.getSimpleName());

    ClassPath.ClassInfo multiDigitLocalWithDigits = new ClassPath.ClassInfo(
        baseUrl, "io/cdap/cdap/Outer$23Local2Helper.class", cl);
    Assert.assertEquals("Local2Helper", multiDigitLocalWithDigits.getSimpleName());
  }

  @Test
  public void testScanDirectoryAndFilterClasses() throws Exception {
    File root = TEMP_FOLDER.newFolder();
    File pkgDir = new File(root, "com/example/scan");
    Assert.assertTrue(pkgDir.mkdirs());
    Files.write(new File(pkgDir, "Top.class").toPath(), new byte[]{0});
    Files.write(new File(pkgDir, "Top$1.class").toPath(), new byte[]{0});
    Files.write(new File(pkgDir, "Top$2Local.class").toPath(), new byte[]{0});
    Files.write(new File(pkgDir, "readme.txt").toPath(),
        "note".getBytes(StandardCharsets.UTF_8));

    try (URLClassLoader loader = new URLClassLoader(new URL[]{root.toURI().toURL()}, null)) {
      ClassPath cp = ClassPath.from(root.toURI(), loader);
      Assert.assertEquals(4, cp.getResources().size());
      Assert.assertEquals(3, cp.getAllClasses().size());
      Assert.assertEquals(1, cp.getTopLevelClasses("com.example.scan").size());

      Set<String> simpleNames = cp.getAllClassesRecursive("com.example").stream()
          .map(ClassPath.ClassInfo::getSimpleName)
          .collect(Collectors.toSet());
      Assert.assertTrue(simpleNames.contains("Top"));
      Assert.assertTrue(simpleNames.contains(""));
      Assert.assertTrue(simpleNames.contains("Local"));
    }
  }
}
