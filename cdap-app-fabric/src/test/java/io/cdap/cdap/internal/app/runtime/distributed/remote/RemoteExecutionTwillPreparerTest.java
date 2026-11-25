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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
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
    Assert.assertTrue(javaVersionCheck.get(0).startsWith("if [ -n \"${JAVA_HOME:-}\" ]"));
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

  @Test
  public void testFallsBackToPathWhenJavaHomeIsUnset() throws Exception {
    // The script runs with "set -u", so an unset JAVA_HOME must not abort it.
    File pathJava = createFakeJavaHome("openjdk version \"11.0.21\" 2023-10-17");
    Assert.assertEquals(0, runCheck(null, pathJava));

    pathJava = createFakeJavaHome("openjdk version \"1.8.0_392\"");
    Assert.assertEquals(1, runCheck(null, pathJava));
  }

  @Test
  public void testJavaHomeTakesPrecedenceOverPath() throws Exception {
    File javaHome = createFakeJavaHome("openjdk version \"11.0.21\" 2023-10-17");
    File pathJava = createFakeJavaHome("openjdk version \"1.8.0_392\"");
    Assert.assertEquals(0, runCheck(javaHome, pathJava));
  }

  /**
   * Runs the inlined Java version check against a fake {@code JAVA_HOME/bin/java} that prints the
   * given lines to stderr, and returns the exit code of the script.
   */
  private int runCheck(String... javaVersionOutput) throws Exception {
    return runCheck(createFakeJavaHome(javaVersionOutput), null);
  }

  /**
   * Runs the inlined Java version check under {@code set -u} and returns the exit code of the
   * script.
   *
   * @param javaHome the {@code JAVA_HOME} to set, or {@code null} to leave it unset
   * @param pathJavaHome a fake Java home whose {@code bin} directory is put first on the
   *     {@code PATH}, or {@code null} to keep the {@code PATH} as is
   */
  private int runCheck(@Nullable File javaHome, @Nullable File pathJavaHome) throws Exception {
    List<String> script = new ArrayList<>();
    script.add("#!/bin/bash");
    script.add("set -u");
    script.addAll(javaVersionCheck);
    File scriptFile = TEMP_FOLDER.newFile();
    Files.write(scriptFile.toPath(), script, StandardCharsets.UTF_8);

    ProcessBuilder builder = new ProcessBuilder(Arrays.asList("bash", scriptFile.getAbsolutePath()))
        .redirectErrorStream(true);
    Map<String, String> env = builder.environment();
    if (javaHome == null) {
      env.remove("JAVA_HOME");
    } else {
      env.put("JAVA_HOME", javaHome.getAbsolutePath());
    }
    if (pathJavaHome != null) {
      env.put("PATH", new File(pathJavaHome, "bin").getAbsolutePath() + File.pathSeparator
          + env.getOrDefault("PATH", ""));
    }
    Process process = builder.start();
    try (InputStream is = process.getInputStream()) {
      ByteStreams.toByteArray(is);
    }
    Assert.assertTrue(process.waitFor(30, TimeUnit.SECONDS));
    return process.exitValue();
  }

  /**
   * Creates a fake Java home with a {@code bin/java} script that prints the given lines to stderr.
   */
  private File createFakeJavaHome(String... javaVersionOutput) throws IOException {
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
    return javaHome;
  }
}
