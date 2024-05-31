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

package io.cdap.cdap.data.tools;

import com.google.inject.Guice;
import com.google.inject.Injector;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.DFSLocationModule;
import io.cdap.cdap.data2.util.hbase.CoprocessorManager;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtil;
import io.cdap.cdap.data2.util.hbase.HBaseTableUtilFactory;
import io.cdap.cdap.security.impersonation.SecurityUtil;
import java.io.IOException;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool to build and upload required HBase coprocessors. Should be run before the CDAP master starts
 * up. Should also be run on slave clusters to ensure that the required coprocessors exist on HDFS.
 */
public class CoprocessorBuildTool {

  private static final Logger LOG = LoggerFactory.getLogger(CoprocessorBuildTool.class);

  public static void main(final String[] args) throws ParseException {

    Options options = new Options()
        .addOption(new Option("h", "help", false, "Print this usage message."))
        .addOption(
            new Option("f", "force", false, "Overwrites any coprocessors that already exist."));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);
    String[] commandArgs = commandLine.getArgs();

    // if help is an option, or if there isn't a single 'ensure' command, print usage and exit.
    if (commandLine.hasOption("h") || commandArgs.length != 1 || !"check".equalsIgnoreCase(
        commandArgs[0])) {
      HelpFormatter helpFormatter = new HelpFormatter();
      helpFormatter.printHelp(
          CoprocessorBuildTool.class.getName() + " check",
          "Checks that HBase coprocessors required by CDAP are loaded onto HDFS. "
              + "If not, the coprocessors are built and placed on HDFS.", options, "");
      System.exit(0);
    }

    boolean overwrite = commandLine.hasOption("f");

    CConfiguration cConf = CConfiguration.create();
    Configuration hConf = HBaseConfiguration.create();

    Injector injector = Guice.createInjector(
        new ConfigModule(cConf, hConf),
        new DFSLocationModule()
    );

    try {
      SecurityUtil.loginForMasterService(cConf);
    } catch (Exception e) {
      LOG.error("Failed to login as CDAP user", e);
      System.exit(1);
    }

    LocationFactory locationFactory = injector.getInstance(LocationFactory.class);
    HBaseTableUtil tableUtil = new HBaseTableUtilFactory(cConf).get();
    CoprocessorManager coprocessorManager = new CoprocessorManager(cConf, locationFactory,
        tableUtil);

    try {
      Location location = coprocessorManager.ensureCoprocessorExists(overwrite);
      LOG.info("coprocessor exists at {}.", location);
    } catch (IOException e) {
      LOG.error("Unable to build and upload coprocessor jars.", e);
      System.exit(1);
    }
  }

}
