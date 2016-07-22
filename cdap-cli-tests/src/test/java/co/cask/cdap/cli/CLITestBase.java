/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.cli;

import co.cask.cdap.cli.util.InstanceURIParser;
import co.cask.cdap.cli.util.table.CsvTableRenderer;
import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.client.config.ConnectionConfig;
import co.cask.cdap.security.authorization.AuthorizerInstantiator;
import co.cask.common.cli.CLI;
import com.google.common.base.Function;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import javax.annotation.Nullable;

/**
 * Base class for CLI Tests.
 */
public class CLITestBase {

  @ClassRule
  public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

  protected static CLIConfig createCLIConfig(URI standaloneUri) throws Exception {
    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parse(standaloneUri.toString());
    ClientConfig clientConfig = new ClientConfig.Builder().setConnectionConfig(connectionConfig).build();
    clientConfig.setAllTimeouts(60000);
    return new CLIConfig(clientConfig, System.out, new CsvTableRenderer());
  }

  protected static void testCommandOutputContains(CLI cli, String command,
                                                  final String expectedOutput) throws Exception {
    testCommand(cli, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertTrue(String.format("Expected output '%s' to contain '%s'", output, expectedOutput),
                          output != null && output.contains(expectedOutput));
        return null;
      }
    });
  }

  protected static void testCommandOutputNotContains(CLI cli, String command,
                                                   final String expectedOutput) throws Exception {
    testCommand(cli, command, new Function<String, Void>() {
      @Nullable
      @Override
      public Void apply(@Nullable String output) {
        Assert.assertTrue(String.format("Expected output '%s' to not contain '%s'", output, expectedOutput),
                          output != null && !output.contains(expectedOutput));
        return null;
      }
    });
  }

  protected static void testCommand(CLI cli, String command, Function<String, Void> outputValidator) throws Exception {
    String output = getCommandOutput(cli, command);
    outputValidator.apply(output);
  }

  protected static String getCommandOutput(CLI cli, String command) throws Exception {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream)) {
      cli.execute(command, printStream);
      return outputStream.toString();
    }
  }
}
