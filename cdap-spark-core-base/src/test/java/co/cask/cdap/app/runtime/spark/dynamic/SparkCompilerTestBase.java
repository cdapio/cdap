/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.dynamic;

import co.cask.cdap.api.spark.dynamic.CompilationFailureException;
import co.cask.cdap.api.spark.dynamic.InterpretFailureException;
import co.cask.cdap.api.spark.dynamic.SparkCompiler;
import co.cask.cdap.api.spark.dynamic.SparkInterpreter;
import com.google.common.base.Joiner;
import com.google.common.io.Files;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.collection.JavaConversions;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

/**
 * Unit test base for {@link SparkCompiler}.
 */
public abstract class SparkCompilerTestBase {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testCompiler() throws Exception {
    // Jar file for saving the TestClass
    File testClassJar = new File(TEMP_FOLDER.newFolder(), "testClass.jar");

    try (SparkCompiler compiler = createCompiler()) {
      StringWriter writer = new StringWriter();

      // Compile a class that write out array to a file
      try (PrintWriter sourceWriter = new PrintWriter(writer, true)) {
        sourceWriter.println("package co.cask.cdap.test");
        sourceWriter.println("import java.io._");
        sourceWriter.println("class TestClass(outputFile: File) {");
        sourceWriter.println("  def write(args: Array[String]): Unit = {");
        sourceWriter.println("    val writer = new PrintWriter(new FileWriter(outputFile), true)");
        sourceWriter.println("    try {");
        sourceWriter.println("      args.foreach(writer.println)");
        sourceWriter.println("    } finally {");
        sourceWriter.println("      writer.close()");
        sourceWriter.println("    }");
        sourceWriter.println("  }");
        sourceWriter.println("}");
      }
      compiler.compile(writer.toString());
      compiler.saveAsJar(testClassJar);
    }

    // Jar file for saving the TestMain
    File mainClassJar = new File(TEMP_FOLDER.newFolder(), "testMain.jar");
    try (SparkCompiler compiler = createCompiler()) {
      // Add the TestClass jar as dependency
      compiler.addDependencies(JavaConversions.asScalaBuffer(Collections.singletonList(testClassJar)));

      // Compile a main class
      StringWriter writer = new StringWriter();
      try (PrintWriter sourceWriter = new PrintWriter(writer, true)) {
        sourceWriter.println("package co.cask.cdap.test");
        sourceWriter.println("import java.io._");
        sourceWriter.println("object TestMain {");
        sourceWriter.println("  def main(args: Array[String]): Unit = {");
        sourceWriter.println("    new TestClass(new File(args(0))).write(args.slice(1, args.length))");
        sourceWriter.println("  }");
        sourceWriter.println("}");
      }
      compiler.compile(writer.toString());
      compiler.saveAsJar(mainClassJar);
    }

    // Create an Interpreter
    SparkInterpreter interpreter = createInterpreter();
    interpreter.addDependencies(JavaConversions.asScalaBuffer(Arrays.asList(testClassJar, mainClassJar)));

    // Call the `TestMain.main` method
    File outputFile = TEMP_FOLDER.newFile();
    interpreter.addImports(JavaConversions.asScalaBuffer(Collections.singletonList("co.cask.cdap.test.TestMain")));
    interpreter.bind("output", File.class.getName(), outputFile,
                     JavaConversions.asScalaBuffer(Collections.<String>emptyList()));
    interpreter.interpret("TestMain.main(Array(output.getAbsolutePath(), \"a\", \"b\", \"c\"))");

    // The main method will write to output file with the rest of the arguments, each on a new line
    String content = Files.toString(outputFile, StandardCharsets.UTF_8).trim();
    Assert.assertEquals(content, Joiner.on(System.getProperty("line.separator")).join("a", "b", "c"));

    interpreter.interpret("val num = 10 + 10");
    Assert.assertEquals(20, interpreter.getValue("num").get());
    Assert.assertTrue(interpreter.getValue("num1").isEmpty());

    // Use the ClassLoader to load the TestMain class and call it using reflection
    File newOutputFile = TEMP_FOLDER.newFile();
    Class<?> testMainClass = interpreter.getClassLoader().loadClass("co.cask.cdap.test.TestMain");
    testMainClass.getDeclaredMethod("main", String[].class).invoke(null, new Object[] { new String[] {
      newOutputFile.getAbsolutePath(), "x", "y", "z"
    }});

    // Verify the new output file content.
    content = Files.toString(newOutputFile, StandardCharsets.UTF_8).trim();
    Assert.assertEquals(content, Joiner.on(System.getProperty("line.separator")).join("x", "y", "z"));
  }

  @Test(expected = CompilationFailureException.class)
  public void testCompilationFailure() throws Exception {
    try (SparkCompiler compiler = createCompiler()) {
      compiler.compile("something random");
    }
  }

  @Test(expected = InterpretFailureException.class)
  public void testInterpretFailure() throws Exception {
    try (SparkInterpreter interpreter = createInterpreter()) {
      interpreter.interpret("String str = \"abc\";");   // this is Java code, not valid Scala
    }
  }

  protected abstract SparkCompiler createCompiler() throws Exception;

  protected abstract SparkInterpreter createInterpreter() throws Exception;
}
