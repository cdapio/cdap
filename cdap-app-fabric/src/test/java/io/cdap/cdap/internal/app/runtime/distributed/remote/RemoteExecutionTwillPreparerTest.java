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

package io.cdap.cdap.internal.app.runtime.distributed.remote;

import com.google.common.collect.ImmutableMap;
import io.cdap.cdap.runtime.spi.ssh.PortForwarding;
import io.cdap.cdap.runtime.spi.ssh.RemotePortForwarding;
import io.cdap.cdap.runtime.spi.ssh.SSHProcess;
import io.cdap.cdap.runtime.spi.ssh.SSHSession;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.twill.internal.JvmOptions;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link RemoteExecutionTwillPreparer}.
 */
public class RemoteExecutionTwillPreparerTest {

  @Test
  public void testGenerateLaunchScriptQuotesOptionsAndEnv() {
    JvmOptions jvmOptions = new JvmOptions(
        "-XX:+UseG1GC -Xloggc:<LOG_DIR>/gc.log -Dkey1=val1 -Dkey2=\"val with space\" "
            + "-Dhttp.nonProxyHosts=es-cluster*|cdap-hdf-*|127.0.0.1|localhost",
        Collections.emptyMap(),
        JvmOptions.DebugOptions.NO_DEBUG);
    String script = RemoteExecutionTwillPreparer.generateLaunchScript(
        "run-123",
        "/var/tmp/cdap/run-123",
        "DataPipelineWorkflow",
        1024,
        jvmOptions,
        ImmutableMap.of("ENV_VAR", "val with space and $HOME"),
        "user/host@REALM",
        "/etc/security/keytabs/user.keytab");

    Assert.assertTrue(script.contains("export ENV_VAR='val with space and $HOME'"));
    Assert.assertTrue(script.contains("export TWILL_RUNNABLE_NAME='DataPipelineWorkflow'"));
    Assert.assertTrue(script.contains(
        "'-XX:+UseG1GC' '-Xloggc:/var/tmp/cdap/run-123/logs/gc.log' "
            + "'-Dkey1=val1' '-Dkey2=val with space' "
            + "'-Dhttp.nonProxyHosts=es-cluster*|cdap-hdf-*|127.0.0.1|localhost' -Xmx1024m"));
    Assert.assertTrue(script.contains("'--runId=run-123'"));
    Assert.assertTrue(script.contains("'--kerberos.principal=user/host@REALM'"));
    Assert.assertTrue(script.contains("'--kerberos.keytab=/etc/security/keytabs/user.keytab'"));
  }

  @Test
  public void testGenerateLaunchScriptWithNullExtraOptsAndQuotedKerberosMetachars() {
    JvmOptions jvmOptions = new JvmOptions(
        null, Collections.emptyMap(), JvmOptions.DebugOptions.NO_DEBUG);
    String script = RemoteExecutionTwillPreparer.generateLaunchScript(
        "run-123",
        "/var/tmp/cdap/run-123",
        "DataPipelineWorkflow",
        512,
        jvmOptions,
        Collections.emptyMap(),
        "user/host'1@REALM",
        "/etc/security/keytabs/user$(id).keytab");

    Assert.assertTrue(script.contains(
        "-cp /var/tmp/cdap/run-123/launcher.jar -Xmx512m "));
    Assert.assertTrue(script.contains(
        "'--kerberos.principal=user/host'\\''1@REALM'"));
    Assert.assertTrue(script.contains(
        "'--kerberos.keytab=/etc/security/keytabs/user$(id).keytab'"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGenerateLaunchScriptRejectsOnOutOfMemoryError() {
    JvmOptions jvmOptions = new JvmOptions(
        "-XX:OnOutOfMemoryError=/bin/id",
        Collections.emptyMap(),
        JvmOptions.DebugOptions.NO_DEBUG);
    RemoteExecutionTwillPreparer.generateLaunchScript(
        "run-123",
        "/var/tmp/cdap/run-123",
        "DataPipelineWorkflow",
        1024,
        jvmOptions,
        Collections.emptyMap(),
        null,
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGenerateLaunchScriptRejectsCommandSubstitution() {
    JvmOptions jvmOptions = new JvmOptions(
        "-Dfoo=$(/bin/id)",
        Collections.emptyMap(),
        JvmOptions.DebugOptions.NO_DEBUG);
    RemoteExecutionTwillPreparer.generateLaunchScript(
        "run-123",
        "/var/tmp/cdap/run-123",
        "DataPipelineWorkflow",
        1024,
        jvmOptions,
        Collections.emptyMap(),
        null,
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGenerateLaunchScriptRejectsInvalidEnvKey() {
    JvmOptions jvmOptions = new JvmOptions(
        "", Collections.emptyMap(), JvmOptions.DebugOptions.NO_DEBUG);
    RemoteExecutionTwillPreparer.generateLaunchScript(
        "run-123",
        "/var/tmp/cdap/run-123",
        "DataPipelineWorkflow",
        1024,
        jvmOptions,
        Collections.singletonMap("BAD_ENV;id", "val"),
        null,
        null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGenerateLaunchScriptRejectsNewlineInEnvValue() {
    JvmOptions jvmOptions = new JvmOptions(
        "", Collections.emptyMap(), JvmOptions.DebugOptions.NO_DEBUG);
    RemoteExecutionTwillPreparer.generateLaunchScript(
        "run-123",
        "/var/tmp/cdap/run-123",
        "DataPipelineWorkflow",
        1024,
        jvmOptions,
        Collections.singletonMap("SAFE_KEY", "line1\nid"),
        null,
        null);
  }

  @Test
  public void testValidateKerberosQuotesKeytabAndRejectsNewline() {
    List<String> executedCommands = new ArrayList<>();
    SSHSession fakeSession = new FakeSSHSession(executedCommands, "user/host@REALM");

    RemoteExecutionTwillPreparer.validateKerberos(
        "user/host@REALM", "/tmp/my keytab;id'1", fakeSession);
    Assert.assertEquals(
        Arrays.asList("which klist", "'/usr/bin/klist' -k -t '/tmp/my keytab;id'\\''1'"),
        executedCommands);

    try {
      RemoteExecutionTwillPreparer.validateKerberos(
          "user/host@REALM", "/tmp/keytab\nid", fakeSession);
      Assert.fail("Expected IllegalArgumentException for newline in keytab");
    } catch (IllegalArgumentException expected) {
      // expected
    }
  }

  private static final class FakeSSHSession implements SSHSession {
    private final List<String> executedCommands;
    private final String klistOutput;

    FakeSSHSession(List<String> executedCommands, String klistOutput) {
      this.executedCommands = executedCommands;
      this.klistOutput = klistOutput;
    }

    @Override
    public boolean isAlive() {
      return true;
    }

    @Override
    public InetSocketAddress getAddress() {
      return new InetSocketAddress("127.0.0.1", 22);
    }

    @Override
    public String getUsername() {
      return "cdap";
    }

    @Override
    public SSHProcess execute(List<String> commands) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public String executeAndWait(List<String> commands) throws IOException {
      String joined = String.join(" ", commands);
      executedCommands.add(joined);
      if ("which klist".equals(joined)) {
        return "/usr/bin/klist\n";
      }
      return klistOutput;
    }

    @Override
    public void copy(Path sourceFile, String targetPath) throws IOException {
    }

    @Override
    public void copy(InputStream input, String targetPath, String targetName, long size,
        int permission, @Nullable Long lastAccessTime, @Nullable Long lastModifiedTime)
        throws IOException {
    }

    @Override
    public PortForwarding createLocalPortForward(String targetHost, int targetPort,
        int originatePort, PortForwarding.DataConsumer dataConsumer) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public RemotePortForwarding createRemotePortForward(int remotePort, int localPort)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
    }
  }
}
