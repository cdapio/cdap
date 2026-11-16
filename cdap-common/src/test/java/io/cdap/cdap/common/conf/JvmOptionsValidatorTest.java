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

package io.cdap.cdap.common.conf;

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link JvmOptionsValidator}.
 */
public class JvmOptionsValidatorTest {

  @Test
  public void testValidUserJvmOptions() {
    JvmOptionsValidator.validateUserJvmOptions(null);
    JvmOptionsValidator.validateUserJvmOptions("");
    JvmOptionsValidator.validateUserJvmOptions("   ");
    JvmOptionsValidator.validateUserJvmOptions("-Dkey1=val1 -Dkey2=val2");
    JvmOptionsValidator.validateUserJvmOptions(
        "-Dkey1=value1 -Dkey2=\"value with space\" -Dkey3=value3=value4 -Dkey5=\"value5\\\"\"");
    JvmOptionsValidator.validateUserJvmOptions(
        "-Dhttp.proxyHost=10.10.1.5 -Dhttp.proxyPort=3128 -Dhttps.proxyHost=10.10.1.5 "
            + "-Dhttps.proxyPort=3128 "
            + "-Dhttp.nonProxyHosts=es-cluster*|cdap-hdf-*|127.0.0.1|localhost "
            + "-Dcom.google.api.client.should_use_proxy=true");
    JvmOptionsValidator.validateUserJvmOptions(
        "-XX:+UseG1GC -XX:MaxDirectMemorySize=1024m -Dfile.encoding=UTF-8");
  }

  @Test
  public void testSplitJvmOptions() {
    List<String> tokens = JvmOptionsValidator.splitJvmOptions(
        "-Dkey1=value1 -Dkey2=\"value with space\" -Dkey3='single quoted' -Dkey4=\"escaped\\\"quote\"");
    Assert.assertEquals(
        Arrays.asList(
            "-Dkey1=value1",
            "-Dkey2=value with space",
            "-Dkey3=single quoted",
            "-Dkey4=escaped\"quote"),
        tokens);
  }

  @Test
  public void testRejectsCommandExecutionHooks() {
    assertInvalidUserOpts("-XX:OnOutOfMemoryError=/bin/id");
    assertInvalidUserOpts("-XX:OnOutOfMemoryError=\"kill -9 %p\"");
    assertInvalidUserOpts("-XX:+OnOutOfMemoryError=/bin/id");
    assertInvalidUserOpts("-XX:OnError=/bin/sh");
    assertInvalidUserOpts("-XX:AbortVMOnException=java.lang.Exception");
    assertInvalidUserOpts("-XX:AbortVMOnExceptionMessage=msg");
    assertInvalidUserOpts("-XX:VMOptionsFile=/tmp/vmopts");
    assertInvalidUserOpts("-XX:Flags=/tmp/flags");
    assertInvalidUserOpts(
        "-Dcustom.injected.flag=TEST -XX:OnOutOfMemoryError=/bin/id -Xmx8m");

    assertInvalidLauncherOpts("-XX:OnOutOfMemoryError=/bin/id");
    assertInvalidLauncherOpts("-XX:OnError=/bin/sh");
    assertInvalidLauncherOpts("-XX:AbortVMOnException=java.lang.Exception");
    assertInvalidLauncherOpts("-XX:AbortVMOnExceptionMessage=msg");
    assertInvalidLauncherOpts("-XX:VMOptionsFile=/tmp/vmopts");
    assertInvalidLauncherOpts("-XX:Flags=/tmp/flags");
  }

  @Test
  public void testRejectsAgentsAndBootClasspath() {
    assertInvalidUserOpts("-javaagent:/tmp/evil.jar");
    assertInvalidUserOpts("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
    assertInvalidUserOpts("-agentpath:/tmp/libagent.so");
    assertInvalidUserOpts("-Xrunhprof");
    assertInvalidUserOpts("-Xbootclasspath:/tmp/evil.jar");
    assertInvalidUserOpts("-Xbootclasspath/p:/tmp/evil.jar");
    assertInvalidUserOpts("-Xbootclasspath/a:/tmp/evil.jar");
    assertInvalidUserOpts("--patch-module=java.base=/tmp/evil");
    assertInvalidUserOpts("-Xpatch:/tmp/evil");
    assertInvalidUserOpts("-Djdk.instrument.traceUsage=true");
    assertInvalidUserOpts("-Djdk.instrument=true");
    assertInvalidUserOpts("-Djdk.attach.allowAttachSelf=true");
    assertInvalidUserOpts("-XX:+StartAttachListener");
    assertInvalidUserOpts("-XX:-DisableAttachMechanism");
    assertInvalidUserOpts("-XX:JVMCILibPath=/tmp");
    assertInvalidUserOpts("-XX:+UseJVMCINativeLibrary");
    assertInvalidUserOpts("-XX:+EnableJVMCI");
    assertInvalidUserOpts("-XX:Tool=evil");

    assertInvalidLauncherOpts("-javaagent:/tmp/evil.jar");
    assertInvalidLauncherOpts("-agentlib:jdwp=transport=dt_socket");
    assertInvalidLauncherOpts("-agentpath:/tmp/libagent.so");
    assertInvalidLauncherOpts("-Xrunhprof");
    assertInvalidLauncherOpts("-Xbootclasspath/p:/tmp/evil.jar");
    assertInvalidLauncherOpts("--patch-module=java.base=/tmp/evil");
    assertInvalidLauncherOpts("-Xpatch:/tmp/evil");
    assertInvalidLauncherOpts("-XX:+StartAttachListener");
    assertInvalidLauncherOpts("-XX:-DisableAttachMechanism");
    assertInvalidLauncherOpts("-XX:JVMCILibPath=/tmp");
    assertInvalidLauncherOpts("-XX:Tool=evil");
    assertInvalidLauncherOpts("-Djdk.instrument.traceUsage=true");
    assertInvalidLauncherOpts("-Djdk.attach.allowAttachSelf=true");
    assertInvalidLauncherOpts("-Dcom.sun.management.jmxremote");
    assertInvalidLauncherOpts("-Dcom.sun.management.jmxremote.port=9999");
    assertInvalidLauncherOpts("-Djava.system.class.loader=EvilLoader");
    assertInvalidLauncherOpts("-Dcom.sun.jndi.ldap.object.trustURLCodebase=true");
  }

  @Test
  public void testRejectsFileWriteAndDumpOptions() {
    assertInvalidUserOpts("-XX:ErrorFile=/tmp/hs_err.log");
    assertInvalidUserOpts("-XX:HeapDumpPath=/tmp/dump.hprof");
    assertInvalidUserOpts("-XX:+HeapDumpOnOutOfMemoryError");
    assertInvalidUserOpts("-XX:+HeapDumpBeforeFullGC");
    assertInvalidUserOpts("-XX:+HeapDumpAfterFullGC");
    assertInvalidUserOpts("-XX:LogFile=/tmp/vm.log");
    assertInvalidUserOpts("-Xlog:gc*:file=/tmp/gc.log");
    assertInvalidUserOpts("-Xloggc:/tmp/gc.log");
    assertInvalidUserOpts("-XX:+LogVMOutput");
    assertInvalidUserOpts("-XX:PerfDataSaveFile=/tmp/perf");
    assertInvalidUserOpts("-XX:+PerfDataSaveToFile");
    assertInvalidUserOpts("-XX:ReplayDataFile=/tmp/replay");
    assertInvalidUserOpts("-XX:AllocateHeapAt=/tmp");
    assertInvalidUserOpts("-XX:PauseAtStartupFile=/tmp/pause");
    assertInvalidUserOpts("-XX:+PauseAtStartup");
    assertInvalidUserOpts("-XX:StartFlightRecording=filename=/tmp/rec.jfr");
    assertInvalidUserOpts("-XX:FlightRecorderOptions=repository=/tmp");
    assertInvalidUserOpts("-XX:DumpLoadedClassList=/tmp/classes.lst");
    assertInvalidUserOpts("-XX:SharedArchiveFile=/tmp/app.jsa");
    assertInvalidUserOpts("-XX:ArchiveClassesAtExit=/tmp/app.jsa");
    assertInvalidUserOpts("-XX:DynamicDumpSharedSpaces");
    assertInvalidUserOpts("-Xshare:dump");
    assertInvalidUserOpts("-XX:CompileCommand=print,*.*");
    assertInvalidUserOpts("-XX:CompileCommandFile=/tmp/hotspot_compiler");
  }

  @Test
  public void testRejectsHeapAndClasspathOverrides() {
    assertInvalidUserOpts("-Xmx8m");
    assertInvalidUserOpts("-Xms8m");
    assertInvalidUserOpts("-Xss256k");
    assertInvalidUserOpts("-Xmn8m");
    assertInvalidUserOpts("-Xminf0.1");
    assertInvalidUserOpts("-Xmaxf0.5");
    assertInvalidUserOpts("-XX:MaxHeapSize=8m");
    assertInvalidUserOpts("-XX:InitialHeapSize=8m");
    assertInvalidUserOpts("-XX:MinHeapSize=8m");
    assertInvalidUserOpts("-XX:NewSize=8m");
    assertInvalidUserOpts("-XX:MaxNewSize=8m");
    assertInvalidUserOpts("-XX:MaxMetaspaceSize=8m");
    assertInvalidUserOpts("-XX:ThreadStackSize=256");
    assertInvalidUserOpts("-XX:MaxRAM=8m");
    assertInvalidUserOpts("-XX:MaxRAMPercentage=50.0");
    assertInvalidUserOpts("-XX:InitialRAMPercentage=50.0");
    assertInvalidUserOpts("-XX:MinRAMPercentage=50.0");
    assertInvalidUserOpts("-XX:MaxRAMFraction=2");
    assertInvalidUserOpts("-XX:+ExitOnOutOfMemoryError");
    assertInvalidUserOpts("-XX:+CrashOnOutOfMemoryError");
    assertInvalidUserOpts("-XX:+ShowMessageBoxOnError");
    assertInvalidUserOpts("-XX:SelfDestructTimer=1");
    assertInvalidUserOpts("-cp");
    assertInvalidUserOpts("-cp -Dfoo=bar");
    assertInvalidUserOpts("-classpath");
    assertInvalidUserOpts("--class-path=/tmp/evil.jar");
    assertInvalidUserOpts("-jar");
    assertInvalidUserOpts("-m");
    assertInvalidUserOpts("--module=evil.mod");
    assertInvalidUserOpts("--module-path=/tmp/mods");
    assertInvalidUserOpts("-p");
    assertInvalidUserOpts("--upgrade-module-path=/tmp/mods");
    assertInvalidUserOpts("@/tmp/argfile");
    assertInvalidUserOpts("io.cdap.EvilMain");
  }

  @Test
  public void testRejectsRestrictedSystemProperties() {
    assertInvalidUserOpts("-Djava.security.manager");
    assertInvalidUserOpts("-Djava.security.policy=/tmp/open.policy");
    assertInvalidUserOpts("-Dcom.sun.management.jmxremote=true");
    assertInvalidUserOpts("-Dcom.sun.management.jmxremote.port=9999");
    assertInvalidUserOpts("-Djava.rmi.server.codebase=http://evil/classes/");
    assertInvalidUserOpts("-Dsun.rmi.transport.tcp.readTimeout=100");
    assertInvalidUserOpts("-Dcom.sun.jndi.ldap.object.trustURLCodebase=true");
    assertInvalidUserOpts("-Djdk.jndi.object.factoriesFilter=*");
    assertInvalidUserOpts("-Djavax.naming.factory.initial=EvilFactory");
    assertInvalidUserOpts("-Djava.library.path=/tmp");
    assertInvalidUserOpts("-Dsun.boot.library.path=/tmp");
    assertInvalidUserOpts("-Djna.library.path=/tmp");
    assertInvalidUserOpts("-Dorg.xerial.snappy.lib.path=/tmp");
    assertInvalidUserOpts("-Dio.netty.native.workdir=/tmp");
    assertInvalidUserOpts("-Djdk.serialFilter=*");
    assertInvalidUserOpts("-Djava.class.path=/tmp/evil.jar");
    assertInvalidUserOpts("-Djava.ext.dirs=/tmp");
    assertInvalidUserOpts("-Djava.endorsed.dirs=/tmp");
    assertInvalidUserOpts("-Djava.system.class.loader=EvilLoader");
    assertInvalidUserOpts("-Dlogback.configurationFile=/tmp/logback.xml");
    assertInvalidUserOpts("-Dlogback.ContextSelector=EvilSelector");
    assertInvalidUserOpts("-Dlog4j.configuration=file:/tmp/log4j.properties");
    assertInvalidUserOpts("-Dlog4j.configurationFile=/tmp/log4j2.xml");
    assertInvalidUserOpts("-Dlog4j2.configurationFile=/tmp/log4j2.xml");
    assertInvalidUserOpts("-Djava.util.logging.config.file=/tmp/logging.properties");
    assertInvalidUserOpts("-Djava.util.logging.config.class=EvilLoggingConfig");
    assertInvalidUserOpts("-DCDAP_LOG_DIR=/tmp");
    assertInvalidUserOpts("-Duser.dir=/tmp");
    assertInvalidUserOpts("-Djava.io.tmpdir=/tmp");
    assertInvalidUserOpts("-Dhive.classpath=/tmp");
    assertInvalidUserOpts("-D=emptyKey");
    assertInvalidUserOpts("-D");
    assertInvalidUserOpts("-Dspark.yarn.am.extraJavaOptions=-XX:OnOutOfMemoryError=/bin/id");
  }

  @Test
  public void testRejectsShellMetacharactersAndControlChars() {
    assertInvalidUserOpts("-Dfoo=bar\n-Dbar=baz");
    assertInvalidUserOpts("-Dfoo=bar\r-Dbar=baz");
    assertInvalidUserOpts("-Dfoo=bar\0");
    assertInvalidUserOpts("-Dfoo=bar\u007f");
    assertInvalidUserOpts("-Dfoo=$(/bin/id)");
    assertInvalidUserOpts("-Dfoo=${PATH}");
    assertInvalidUserOpts("-Dfoo=$IFS-XX:OnOutOfMemoryError=/bin/id");
    assertInvalidUserOpts("-Dfoo=`/bin/id`");
    assertInvalidUserOpts("-Dfoo=bar;id");
    assertInvalidUserOpts("-Dfoo=\"bar;id\"");
    assertInvalidUserOpts("-Dfoo=bar&id");
    assertInvalidUserOpts("-Dfoo=bar|id");
    assertInvalidUserOpts("-Dhttp.nonProxyHosts=localhost|$(id)");
    assertInvalidUserOpts("-Dhttp.nonProxyHosts=localhost||id");
    assertInvalidUserOpts("-Dhttp.nonProxyHosts=|localhost");
    assertInvalidUserOpts("-Dhttp.nonProxyHosts=localhost|");
    assertInvalidUserOpts("-Dhttp.nonProxyHosts=\"localhost | id\"");
    assertInvalidUserOpts("-Dfoo=bar>/tmp/pwn");
    assertInvalidUserOpts("-Dfoo=bar</tmp/pwn");
    assertInvalidUserOpts("-Dfoo=\"unclosed quote");
    assertInvalidUserOpts("-Dfoo='unclosed single quote");
    assertInvalidUserOpts("-Dfoo=\\\" -XX:OnOutOfMemoryError=/bin/id -Dbar=\"");
    assertInvalidUserOpts("-Dfoo=\"bar -XX:OnOutOfMemoryError=/bin/id\"");
    assertInvalidUserOpts("-Dfoo=\"bar @/tmp/argfile\"");
    assertInvalidUserOpts("-D\"foo -XX:OnOutOfMemoryError=/bin/id\"=bar");
  }

  @Test
  public void testIsJvmOptionConfigKeyAndMapBoundaries() {
    JvmOptionsValidator.validateJvmOptsMap(null);
    JvmOptionsValidator.validateJvmOptsMap(Collections.emptyMap());

    Assert.assertFalse(JvmOptionsValidator.isJvmOptionConfigKey(null));
    Assert.assertFalse(JvmOptionsValidator.isJvmOptionConfigKey(""));
    Assert.assertFalse(JvmOptionsValidator.isJvmOptionConfigKey("   "));
    Assert.assertFalse(JvmOptionsValidator.isJvmOptionConfigKey("unrelated.argument"));

    for (String matchingKey : Arrays.asList(
        "system.program.jvm.opts",
        "SYSTEM.PROGRAM.JVM.OPTS",
        "workflow.DataPipelineWorkflow.system.program.jvm.opts",
        Constants.AppFabric.PROGRAM_JVM_OPTS,
        "system.namespace.config." + Constants.AppFabric.PROGRAM_JVM_OPTS,
        Constants.AppFabric.PROGRAM_JVM_OPTS_PREFIX + "DataPipelineWorkflow",
        "workflow." + Constants.AppFabric.PROGRAM_JVM_OPTS_PREFIX + "DataPipelineWorkflow",
        "spark.driver.extraJavaOptions",
        "spark.executor.extraJavaOptions",
        "spark.yarn.am.extraJavaOptions",
        "spark.driver.defaultJavaOptions",
        "mapreduce.map.java.opts",
        "mapreduce.reduce.java.opts",
        "mapred.child.java.opts",
        "yarn.app.mapreduce.am.command-opts",
        "yarn.app.mapreduce.am.admin-command-opts",
        "twill.jvm.gc.opts",
        "JAVA_TOOL_OPTIONS",
        "spark.executorEnv.JAVA_TOOL_OPTIONS",
        "_JAVA_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "JAVA_OPTS")) {
      Assert.assertTrue("Expected true for key: " + matchingKey,
          JvmOptionsValidator.isJvmOptionConfigKey(matchingKey));
      try {
        JvmOptionsValidator.validateJvmOptsMap(
            Collections.singletonMap(matchingKey, "-XX:OnOutOfMemoryError=/bin/id"));
        Assert.fail("Expected IllegalArgumentException for key: " + matchingKey);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
  }

  @Test
  public void testTokenizerAndValidatorBoundaryConditions() {
    Assert.assertTrue(JvmOptionsValidator.splitJvmOptions(null).isEmpty());
    Assert.assertTrue(JvmOptionsValidator.splitJvmOptions("").isEmpty());
    Assert.assertTrue(JvmOptionsValidator.splitJvmOptions("   \t  ").isEmpty());

    Assert.assertEquals(
        Arrays.asList("-Dkey1=val1", "-Dkey2=a\\b"),
        JvmOptionsValidator.splitJvmOptions("-Dkey1=val1\t'-Dkey2=a\\b'"));

    JvmOptionsValidator.validateUserJvmOptions(
        "-parallel -Dhttps.nonProxyHosts=[::1]|localhost "
            + "-Dspark.yarn.am.extraJavaOptions=-Dhdp.version=2.2.6.0");
    JvmOptionsValidator.validateLauncherJvmOptions(null);
    JvmOptionsValidator.validateLauncherJvmOptions("   ");

    for (String badEnvKey : Arrays.asList(null, "", "1LEADING_DIGIT", "KEY-WITH-DASH")) {
      try {
        JvmOptionsValidator.validateEnvKey(badEnvKey);
        Assert.fail("Expected IllegalArgumentException for env key: " + badEnvKey);
      } catch (IllegalArgumentException expected) {
        // expected
      }
    }
    try {
      JvmOptionsValidator.shellQuote(null);
      Assert.fail("Expected IllegalArgumentException for null shellQuote");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  @Test
  public void testLauncherJvmOptionsAllowsTrustedGcAndLogbackFlags() {
    JvmOptionsValidator.validateLauncherJvmOptions(
        "-XX:+UseG1GC -verbose:gc -Xloggc:/var/log/cdap/gc.log "
            + "-Dlogback.configurationFile=logback-container.xml -DCDAP_LOG_DIR=/var/log/cdap "
            + "-Dkey1=val1");
  }

  @Test
  public void testShellQuoteAndValidateEnvKey() {
    Assert.assertEquals("'simple'", JvmOptionsValidator.shellQuote("simple"));
    Assert.assertEquals("'a'\\''b'", JvmOptionsValidator.shellQuote("a'b"));
    Assert.assertEquals("'$(id) `uname` ; | &'",
        JvmOptionsValidator.shellQuote("$(id) `uname` ; | &"));

    JvmOptionsValidator.validateEnvKey("TWILL_RUNNABLE_NAME");
    JvmOptionsValidator.validateEnvKey("_VALID_ENV_1");

    try {
      JvmOptionsValidator.validateEnvKey("BAD_KEY;id");
      Assert.fail("Expected IllegalArgumentException for invalid env key");
    } catch (IllegalArgumentException expected) {
      // expected
    }

    try {
      JvmOptionsValidator.shellQuote("line1\nline2");
      Assert.fail("Expected IllegalArgumentException for newline in shellQuote");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  private static void assertInvalidUserOpts(String opts) {
    try {
      JvmOptionsValidator.validateUserJvmOptions(opts);
      Assert.fail("Expected IllegalArgumentException for user JVM options: " + opts);
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  private static void assertInvalidLauncherOpts(String opts) {
    try {
      JvmOptionsValidator.validateLauncherJvmOptions(opts);
      Assert.fail("Expected IllegalArgumentException for launcher JVM options: " + opts);
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }
}
