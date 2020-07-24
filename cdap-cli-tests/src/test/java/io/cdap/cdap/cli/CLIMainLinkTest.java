/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.cli;

import io.cdap.cdap.cli.util.InstanceURIParser;
import io.cdap.cdap.cli.util.table.CsvTableRenderer;
import io.cdap.cdap.client.ProgramClient;
import io.cdap.cdap.client.QueryClient;
import io.cdap.cdap.client.app.FakeApp;
import io.cdap.cdap.client.config.ClientConfig;
import io.cdap.cdap.client.config.ConnectionConfig;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.test.XSlowTests;
import io.cdap.common.cli.CLI;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import java.net.URI;

@Category(XSlowTests.class)
public class CLIMainLinkTest extends CLITestBase {

  private static ProgramClient programClient;
  private static QueryClient queryClient;
  private static CLIConfig cliConfig;
  private static CLIMain cliMain;
  private static CLI cli;

  @BeforeClass
  public static void setUpClass() throws Exception {
    cliConfig = createCLIConfigWithURIPrefix(STANDALONE.getBaseURI());
    LaunchOptions launchOptions = new LaunchOptions("", true, true,
                                                    false, null, LaunchOptions.DEFAULT.getUri());
    cliMain = new CLIMain(launchOptions, cliConfig);
    programClient = new ProgramClient(cliConfig.getClientConfig());
    queryClient = new QueryClient(cliConfig.getClientConfig());

    cli = cliMain.getCLI();

    testSetup(cli, STANDALONE, TMP_FOLDER);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    programClient.stopAll(NamespaceId.DEFAULT);
    testCommandOutputContains(cli, "delete app " + FakeApp.NAME, "Successfully deleted app");
    testCommandOutputContains(cli, String.format("delete app %s version %s", FakeApp.NAME, V1_SNAPSHOT),
                              "Successfully deleted app");
  }

  public static CLIConfig createCLIConfigWithURIPrefix(URI standaloneUri) throws Exception {
    ConnectionConfig connectionConfig = InstanceURIParser.DEFAULT.parseInstanceURI(standaloneUri.toString(), null);
    ClientConfig clientConfig = new ClientConfig.Builder().setConnectionConfig(connectionConfig).build();
    clientConfig.setAllTimeouts(60000);
    return new CLIConfig(clientConfig, System.out, new CsvTableRenderer());
  }

  @Override
  CLI getCLI() {
    return cli;
  }

  @Override
  ProgramClient getProgramClient() {
    return programClient;
  }

  @Override
  CLIMain getCliMain() {
    return cliMain;
  }

  @Override
  CLIConfig getCliConfig() {
    return cliConfig;
  }

  @Override
  QueryClient getQueryClient() {
    return queryClient;
  }
}
