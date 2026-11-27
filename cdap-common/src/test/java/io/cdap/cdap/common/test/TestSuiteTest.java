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

package io.cdap.cdap.common.test;

import io.cdap.cdap.common.lang.FilterClassLoader;
import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import javax.tools.JavaCompiler;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.internal.builders.AllDefaultPossibilitiesBuilder;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.JUnitCore;
import org.junit.runner.Request;
import org.junit.runner.Result;
import org.junit.runner.RunWith;
import org.junit.runner.Runner;
import org.junit.runners.Parameterized;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

/**
 * Unit tests for {@link TestSuite} classloader resolution and execution.
 */
public class TestSuiteTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static File compiledFixturesDir;

  // All in-tree fixture classes are abstract with no @Test methods so Surefire's
  // NonAbstractClassFilter never executes them standalone even when CI passes -Dtest='!...'.

  @RunWith(TestRunner.class)
  public abstract static class BaseRunnerFixture {
  }

  public abstract static class InheritedTestRunnerFixture extends BaseRunnerFixture {
  }

  @RunWith(TestRunner.class)
  public abstract static class DirectTestRunnerFixture {
  }

  public abstract static class CustomTestRunner extends TestRunner {
    protected CustomTestRunner(Class<?> testClass) throws InitializationError {
      super(testClass);
    }
  }

  @RunWith(CustomTestRunner.class)
  public abstract static class CustomTestRunnerFixture {
  }

  public abstract static class CustomTestSuite extends TestSuite {
    protected CustomTestSuite(Class<?> suiteClass, RunnerBuilder builder)
        throws InitializationError {
      super(suiteClass, builder);
    }
  }

  @RunWith(Parameterized.class)
  public abstract static class ParameterizedFixture extends BaseRunnerFixture {
  }

  public abstract static class PlainJunitFixture {
  }

  @RunWith(TestSuite.class)
  @Suite.SuiteClasses({PlainJunitFixture.class})
  public abstract static class NestedSuiteFixture {
  }

  @RunWith(CustomTestSuite.class)
  @Suite.SuiteClasses({PlainJunitFixture.class})
  public abstract static class CustomNestedSuiteFixture {
  }

  @BeforeClass
  public static void compileRunnableSuiteFixtures() throws Exception {
    compiledFixturesDir = TEMP_FOLDER.newFolder("suite-fixtures");
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    Assert.assertNotNull("JDK JavaCompiler is required for tests", compiler);

    List<SimpleJavaFileObject> sources = Arrays.asList(
        javaSource("io.cdap.cdap.common.test.fixture.SharedStateSuite",
            "package io.cdap.cdap.common.test.fixture;\n"
                + "import io.cdap.cdap.common.test.TestSuite;\n"
                + "import org.junit.ClassRule;\n"
                + "import org.junit.rules.ExternalResource;\n"
                + "import org.junit.runner.RunWith;\n"
                + "import org.junit.runners.Suite;\n"
                + "@RunWith(TestSuite.class)\n"
                + "@Suite.SuiteClasses({SharedParameterizedChild.class, SharedPlainChild.class})\n"
                + "public class SharedStateSuite {\n"
                + "  public static volatile String sharedValue;\n"
                + "  public static volatile ClassLoader parametersContextClassLoader;\n"
                + "  @ClassRule\n"
                + "  public static final ExternalResource SUITE_RESOURCE = new ExternalResource() {\n"
                + "    @Override protected void before() { sharedValue = \"initialized\"; }\n"
                + "    @Override protected void after() { sharedValue = null; }\n"
                + "  };\n"
                + "}\n"),
        javaSource("io.cdap.cdap.common.test.fixture.SharedParameterizedChild",
            "package io.cdap.cdap.common.test.fixture;\n"
                + "import io.cdap.cdap.common.test.TestSuiteTest;\n"
                + "import java.util.Arrays;\n"
                + "import java.util.Collection;\n"
                + "import org.junit.Assert;\n"
                + "import org.junit.Test;\n"
                + "import org.junit.runner.RunWith;\n"
                + "import org.junit.runners.Parameterized;\n"
                + "@RunWith(Parameterized.class)\n"
                + "public class SharedParameterizedChild extends TestSuiteTest.BaseRunnerFixture {\n"
                + "  private final String param;\n"
                + "  public SharedParameterizedChild(String param) { this.param = param; }\n"
                + "  @Parameterized.Parameters\n"
                + "  public static Collection<Object[]> params() {\n"
                + "    SharedStateSuite.parametersContextClassLoader =\n"
                + "        Thread.currentThread().getContextClassLoader();\n"
                + "    return Arrays.asList(new Object[][] {{\"a\"}, {\"b\"}});\n"
                + "  }\n"
                + "  @Test\n"
                + "  public void testSeesSuiteClassLoaderAndClassRuleState() {\n"
                + "    Assert.assertEquals(\"initialized\", SharedStateSuite.sharedValue);\n"
                + "    Assert.assertSame(SharedStateSuite.class.getClassLoader(),\n"
                + "        getClass().getClassLoader());\n"
                + "    Assert.assertSame(getClass().getClassLoader(),\n"
                + "        Thread.currentThread().getContextClassLoader());\n"
                + "    Assert.assertSame(getClass().getClassLoader(),\n"
                + "        SharedStateSuite.parametersContextClassLoader);\n"
                + "    Assert.assertTrue(\"a\".equals(param) || \"b\".equals(param));\n"
                + "  }\n"
                + "}\n"),
        javaSource("io.cdap.cdap.common.test.fixture.SharedPlainChild",
            "package io.cdap.cdap.common.test.fixture;\n"
                + "import org.junit.Assert;\n"
                + "import org.junit.Test;\n"
                + "public class SharedPlainChild {\n"
                + "  @Test\n"
                + "  public void testSeesSuiteClassLoaderAndClassRuleState() {\n"
                + "    Assert.assertEquals(\"initialized\", SharedStateSuite.sharedValue);\n"
                + "    Assert.assertSame(SharedStateSuite.class.getClassLoader(),\n"
                + "        getClass().getClassLoader());\n"
                + "    Assert.assertSame(getClass().getClassLoader(),\n"
                + "        Thread.currentThread().getContextClassLoader());\n"
                + "  }\n"
                + "}\n"));

    try (StandardJavaFileManager fileManager =
        compiler.getStandardFileManager(null, null, StandardCharsets.UTF_8)) {
      fileManager.setLocation(StandardLocation.CLASS_OUTPUT,
          Arrays.asList(compiledFixturesDir));
      List<String> options = Arrays.asList("-classpath", System.getProperty("java.class.path"));
      Boolean ok = compiler.getTask(null, fileManager, null, options, null, sources).call();
      Assert.assertTrue("Failed to compile test suite fixtures", Boolean.TRUE.equals(ok));
    }
  }

  @Test
  public void testResolveRunnerClassesReloadsNonSuiteManagedChildrenOnly() throws Exception {
    try (URLClassLoader childLoader = createIsolatedTestClassLoader()) {
      Class<?>[] input = new Class<?>[] {
          InheritedTestRunnerFixture.class,
          DirectTestRunnerFixture.class,
          CustomTestRunnerFixture.class,
          NestedSuiteFixture.class,
          CustomNestedSuiteFixture.class,
          ParameterizedFixture.class,
          PlainJunitFixture.class
      };
      Class<?>[] resolved = TestSuite.resolveRunnerClasses(input, childLoader);
      Assert.assertEquals(input.length, resolved.length);

      Assert.assertSame(InheritedTestRunnerFixture.class, resolved[0]);
      Assert.assertSame(DirectTestRunnerFixture.class, resolved[1]);
      Assert.assertSame(CustomTestRunnerFixture.class, resolved[2]);
      Assert.assertSame(NestedSuiteFixture.class, resolved[3]);
      Assert.assertSame(CustomNestedSuiteFixture.class, resolved[4]);

      Assert.assertNotSame(ParameterizedFixture.class, resolved[5]);
      Assert.assertEquals(ParameterizedFixture.class.getName(), resolved[5].getName());
      Assert.assertSame(childLoader, resolved[5].getClassLoader());

      Assert.assertNotSame(PlainJunitFixture.class, resolved[6]);
      Assert.assertEquals(PlainJunitFixture.class.getName(), resolved[6].getName());
      Assert.assertSame(childLoader, resolved[6].getClassLoader());

      Assert.assertEquals(0,
          TestSuite.resolveRunnerClasses(new Class<?>[0], childLoader).length);
    }
  }

  @Test
  public void testSuiteSharesClassLoaderAndClassRuleWithReloadedChildren() throws Exception {
    ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader callerLoader = new URLClassLoader(
            new URL[] {compiledFixturesDir.toURI().toURL()},
            TestSuiteTest.class.getClassLoader());
        URLClassLoader childLoader = createIsolatedTestClassLoader()) {
      Class<?> callerSuiteClass =
          callerLoader.loadClass("io.cdap.cdap.common.test.fixture.SharedStateSuite");
      TestSuite suite = new TestSuite(
          callerSuiteClass,
          new AllDefaultPossibilitiesBuilder(/* canUseSuiteMethod= */ true),
          childLoader);
      Class<?> reloadedSuiteClass = suite.getTestClass().getJavaClass();
      Assert.assertNotSame(callerSuiteClass, reloadedSuiteClass);
      Assert.assertSame(childLoader, reloadedSuiteClass.getClassLoader());
      Assert.assertSame(originalTccl, Thread.currentThread().getContextClassLoader());

      Result result = new JUnitCore().run(Request.runner(suite));
      Assert.assertTrue(result.getFailures().toString(), result.wasSuccessful());
      Assert.assertEquals(3, result.getRunCount());
      Assert.assertNull(reloadedSuiteClass.getField("sharedValue").get(null));
      Assert.assertSame(childLoader,
          reloadedSuiteClass.getField("parametersContextClassLoader").get(null));
      Assert.assertNull(callerSuiteClass.getField("sharedValue").get(null));
      Assert.assertNull(callerSuiteClass.getField("parametersContextClassLoader").get(null));
      Assert.assertSame(originalTccl, Thread.currentThread().getContextClassLoader());
    } finally {
      Thread.currentThread().setContextClassLoader(originalTccl);
    }
  }

  @Test
  public void testConstructorRestoresTcclWhenRunnerBuilderThrows() throws Exception {
    ClassLoader originalTccl = Thread.currentThread().getContextClassLoader();
    try (URLClassLoader childLoader = createIsolatedTestClassLoader()) {
      RunnerBuilder failingBuilder = new RunnerBuilder() {
        @Override
        public Runner runnerForClass(Class<?> testClass) {
          throw new UnsupportedOperationException("unused");
        }

        @Override
        public List<Runner> runners(Class<?> parent, Class<?>[] children)
            throws InitializationError {
          Assert.assertSame(childLoader, Thread.currentThread().getContextClassLoader());
          throw new InitializationError("simulated runner failure");
        }
      };
      try {
        new TestSuite(NestedSuiteFixture.class, failingBuilder, childLoader);
        Assert.fail("Expected InitializationError");
      } catch (InitializationError expected) {
        Assert.assertSame(originalTccl, Thread.currentThread().getContextClassLoader());
      }
    } finally {
      Thread.currentThread().setContextClassLoader(originalTccl);
    }
  }

  @Test
  public void testResolveRunnerClassesWrapsClassNotFound() throws Exception {
    try (URLClassLoader emptyLoader = new URLClassLoader(new URL[0], null)) {
      try {
        TestSuite.resolveRunnerClasses(
            new Class<?>[] {PlainJunitFixture.class}, emptyLoader);
        Assert.fail("Expected InitializationError wrapping ClassNotFoundException");
      } catch (InitializationError expected) {
        Assert.assertEquals(1, expected.getCauses().size());
        Assert.assertTrue(
            "Expected ClassNotFoundException cause, got: " + expected.getCauses().get(0),
            expected.getCauses().get(0) instanceof ClassNotFoundException);
      }
    }
  }

  private static URLClassLoader createIsolatedTestClassLoader() throws Exception {
    URL testClassesUrl = TestSuiteTest.class.getProtectionDomain().getCodeSource().getLocation();
    URL[] urls = compiledFixturesDir != null
        ? new URL[] {compiledFixturesDir.toURI().toURL(), testClassesUrl}
        : new URL[] {testClassesUrl};
    FilterClassLoader junitOnlyParent = new FilterClassLoader(
        TestSuiteTest.class.getClassLoader(),
        new FilterClassLoader.Filter() {
          @Override
          public boolean acceptResource(String resource) {
            return resource.startsWith("org/junit/") || resource.startsWith("org/hamcrest/");
          }

          @Override
          public boolean acceptPackage(String packageName) {
            return packageName.equals("org.junit") || packageName.startsWith("org.junit.")
                || packageName.equals("org.hamcrest") || packageName.startsWith("org.hamcrest.");
          }
        });
    return new URLClassLoader(urls, junitOnlyParent);
  }

  private static SimpleJavaFileObject javaSource(String binaryName, final String source) {
    URI uri = URI.create("string:///" + binaryName.replace('.', '/')
        + SimpleJavaFileObject.Kind.SOURCE.extension);
    return new SimpleJavaFileObject(uri, SimpleJavaFileObject.Kind.SOURCE) {
      @Override
      public CharSequence getCharContent(boolean ignoreEncodingErrors) {
        return source;
      }
    };
  }
}
