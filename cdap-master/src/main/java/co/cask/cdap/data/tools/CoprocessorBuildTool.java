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

package co.cask.cdap.data.tools;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.guice.ConfigModule;
import co.cask.cdap.common.guice.FileContextProvider;
import co.cask.cdap.common.guice.InsecureFileContextLocationFactory;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.data2.util.hbase.CoprocessorManager;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.HBaseTableUtilFactory;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Tool to build and upload required HBase coprocessors. Should be run before the CDAP master starts up.
 * Should also be run on slave clusters to ensure that the required coprocessors exist on HDFS.
 */
public class CoprocessorBuildTool {
  private static final Logger LOG = LoggerFactory.getLogger(CoprocessorBuildTool.class);

  public static void main(final String[] args) throws ParseException {

    Options options = new Options()
      .addOption(new Option("h", "help", false, "Print this usage message."))
      .addOption(new Option("f", "force", false, "Overwrites any coprocessors that already exist."));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);
    String[] commandArgs = commandLine.getArgs();

    // if help is an option, or if there isn't a single 'ensure' command, print usage and exit.
    if (commandLine.hasOption("h") || commandArgs.length != 1 || !"check".equalsIgnoreCase(commandArgs[0])) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(
        CoprocessorBuildTool.class.getName() + " check",
        "Checks that HBase coprocessors required by CDAP are loaded onto HDFS. " +
          "If not, the coprocessors are built and placed on HDFS.", options, "");
      System.exit(0);
    }

    boolean overwrite = commandLine.hasOption("f");

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create();

    Injector injector = Guice.createInjector(
      new ConfigModule(cConf, hConf),
      // for LocationFactory
      new PrivateModule() {
        @Override
        protected void configure() {
          bind(FileContext.class).toProvider(FileContextProvider.class).in(Scopes.SINGLETON);
          expose(LocationFactory.class);
        }

        @Provides
        @Singleton
        private LocationFactory providesLocationFactory(Configuration hConf, CConfiguration cConf, FileContext fc) {
          final String namespace = cConf.get(Constants.CFG_HDFS_NAMESPACE);
          if (UserGroupInformation.isSecurityEnabled()) {
            return new FileContextLocationFactory(hConf, namespace);
          }
          return new InsecureFileContextLocationFactory(hConf, namespace, fc);
        }
      }
    );

    try {
      SecurityUtil.loginForMasterService(cConf);
    } catch (Exception e) {
      LOG.error("Failed to login as CDAP user", e);
      System.exit(1);
    }

    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    CoprocessorManager coprocessorManager = new CoprocessorManager(cConf, locationFactory, tableUtil);

    try {
      Location location = coprocessorManager.ensureCoprocessorExists(overwrite);
      LOG.info("coprocessor exists at {}.", location);
    } catch (IOException e) {
      LOG.error("Unable to build and upload coprocessor jars.", e);
      System.exit(1);
    }
  }

}
