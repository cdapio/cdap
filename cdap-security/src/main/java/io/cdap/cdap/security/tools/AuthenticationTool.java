/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.security.tools;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.guice.ConfigModule;
import io.cdap.cdap.common.guice.IOModule;
import io.cdap.cdap.common.guice.InMemoryDiscoveryModule;
import io.cdap.cdap.common.io.Codec;
import io.cdap.cdap.security.auth.FileBasedKeyManager;
import io.cdap.cdap.security.auth.KeyIdentifier;
import io.cdap.cdap.security.auth.KeyIdentifierCodec;
import io.cdap.cdap.security.guice.SecurityModules;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.Random;

/**
 * A command line tool for authentication.
 */
public class AuthenticationTool {

  public static void main(String[] args) throws ParseException, NoSuchAlgorithmException, IOException {
    Options options = new Options()
      .addOption(new Option("h", "help", false, "Print this usage message."))
      .addOption(new Option("g", "generateKey", true, "Generates a key and save it to a file"));

    CommandLineParser parser = new BasicParser();
    CommandLine commandLine = parser.parse(options, args);

    if (commandLine.hasOption("h") || commandLine.getOptions().length == 0) {
      printUsage(options);
      return;
    }

    if (commandLine.hasOption("g")) {
      String file = commandLine.getOptionValue("g");
      if (file == null) {
        System.out.println("Missing key file");
        printUsage(options);
        return;
      }

      File keyFile = new File(file);

      CConfiguration cConf = CConfiguration.create();
      cConf.set(Constants.Security.CFG_FILE_BASED_KEYFILE_PATH, keyFile.getAbsolutePath());
      cConf.unset(Constants.Zookeeper.QUORUM);

      Injector injector = Guice.createInjector(
        new ConfigModule(cConf),
        new IOModule(),
        new InMemoryDiscoveryModule(),
        SecurityModules.getDistributedModule(cConf));

      Codec<KeyIdentifier> codec = injector.getInstance(Key.get(new TypeLiteral<Codec<KeyIdentifier>>() { }));
      FileBasedKeyManager keyManager = new FileBasedKeyManager(injector.getInstance(CConfiguration.class),
                                                               injector.getInstance(KeyIdentifierCodec.class));

      KeyIdentifier keyIdentifier = keyManager.generateKey(keyManager.createKeyGenerator(),
                                                           new Random().nextInt(Integer.MAX_VALUE));
      try {
        Files.write(keyFile.toPath(), codec.encode(keyIdentifier), StandardOpenOption.CREATE_NEW);
      } catch (FileAlreadyExistsException e) {
        System.err.println("File " + keyFile + " already exists. Please specify a different file path.");
      }
      return;
    }

    printUsage(options);
  }

  private static void printUsage(Options options) {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(AuthenticationTool.class.getSimpleName(), options);
  }
}
