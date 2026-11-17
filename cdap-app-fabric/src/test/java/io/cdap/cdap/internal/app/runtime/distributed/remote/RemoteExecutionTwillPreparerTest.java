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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.io.ByteStreams;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Tests for the Java version check that {@link RemoteExecutionTwillPreparer} inlines into the
 * remote launch script.
 */
public class RemoteExecutionTwillPreparerTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static List<String> javaVersionCheck;

  @BeforeClass
  public static void init() throws IOException {
    javaVersionCheck = RemoteExecutionTwillPreparer.readScriptBody(
        RemoteExecutionTwillPreparerTest.class.getClassLoader(), "javaVersionCheck.sh");
  }

  @Test
  public void testReadScriptBodyDropsLeadingComments() {
    Assert.assertFalse(javaVersionCheck.isEmpty());
    Assert.assertTrue(javaVersionCheck.get(0).startsWith("if [ -n \"${JAVA_HOME}\" ]"));
    for (String line : javaVersionCheck) {
      Assert.assertFalse("Unexpected comment line: " + line, line.startsWith("#"));
    }
  }

  @Test(expected = IOException.class)
  public void testReadScriptBodyMissingResource() throws IOException {
    RemoteExecutionTwillPreparer.readScriptBody(getClass().getClassLoader(), "missing.sh");
  }

  @Test
  public void testJava8IsRejected() throws Exception {
    Assert.assertEquals(1, runCheck("openjdk version \"1.8.0_392\""));
  }

  @Test
  public void testJava11IsAccepted() throws Exception {
    Assert.assertEquals(0, runCheck("openjdk version \"11.0.21\" 2023-10-17"));
  }

  @Test
  public void testPickedUpLineIsSkipped() throws Exception {
    Assert.assertEquals(0, runCheck(
        "Picked up JAVA_TOOL_OPTIONS: -Dversion=\"1.8\"",
        "openjdk version \"17.0.9\" 2023-10-17"));
    Assert.assertEquals(1, runCheck(
        "Picked up JAVA_TOOL_OPTIONS: -Dversion=\"21\"",
        "openjdk version \"1.8.0_392\""));
  }

  @Test
  public void testUnknownVersionIsRejected() throws Exception {
    Assert.assertEquals(1, runCheck("Error: could not find libjava.so"));
  }

  /**
   * Runs the inlined Java version check against a fake {@code JAVA_HOME/bin/java} that prints the
   * given lines to stderr, and returns the exit code of the script.
   */
  private int runCheck(String... javaVersionOutput) throws Exception {
    Assume.assumeFalse(System.getProperty("os.name").toLowerCase().startsWith("windows"));

    File javaHome = TEMP_FOLDER.newFolder();
    File bin = new File(javaHome, "bin");
    Assert.assertTrue(bin.mkdirs());
    List<String> fakeJava = new ArrayList<>();
    fakeJava.add("#!/bin/sh");
    for (String line : javaVersionOutput) {
      fakeJava.add("echo '" + line + "' >&2");
    }
    File java = new File(bin, "java");
    Files.write(java.toPath(), fakeJava, StandardCharsets.UTF_8);
    Assert.assertTrue(java.setExecutable(true));

    List<String> script = new ArrayList<>();
    script.add("#!/bin/bash");
    script.addAll(javaVersionCheck);
    File scriptFile = TEMP_FOLDER.newFile();
    Files.write(scriptFile.toPath(), script, StandardCharsets.UTF_8);

    ProcessBuilder builder = new ProcessBuilder(Arrays.asList("bash", scriptFile.getAbsolutePath()))
        .redirectErrorStream(true);
    builder.environment().put("JAVA_HOME", javaHome.getAbsolutePath());
    Process process = builder.start();
    try (InputStream is = process.getInputStream()) {
      ByteStreams.toByteArray(is);
    }
    Assert.assertTrue(process.waitFor(30, TimeUnit.SECONDS));
    return process.exitValue();
  }
}
