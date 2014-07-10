/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy;

import com.continuuity.api.app.Application;
import com.continuuity.api.app.ApplicationContext;
import com.continuuity.app.ApplicationSpecification;
import com.continuuity.app.DefaultAppConfigurer;
import com.continuuity.app.program.Program;
import com.continuuity.app.program.Programs;
import com.continuuity.internal.app.ApplicationSpecificationAdapter;
import com.continuuity.internal.app.Specifications;
import com.continuuity.internal.io.ReflectionSchemaGenerator;
import com.continuuity.security.ApplicationSecurity;
import com.google.common.base.Charsets;
import com.google.common.io.Files;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.twill.filesystem.HDFSLocationFactory;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilePermission;
import java.io.IOException;
import java.io.Writer;

/**
 * Sandbox JVM allows the configuration phase of an application to be executed
 * within a contained JVM within minimal access to JVM capabilities.
 * <p>
 * Idea is that this piece of code is called in during the configuration phase
 * which happens during deployment and running in the same JVM as the server
 * could be dangerous. Hence, we spin-up a JVM with restricted access to resources
 * and invoke configure on application.
 * </p>
 */
public class SandboxJVM {
  private static final Logger LOG = LoggerFactory.getLogger(SandboxJVM.class);

  /**
   * Main class within the object.
   *
   * @param args specified on command line.
   * @return 0 if successfull; otherwise non-zero.
   */
  public int doMain(String[] args) {
    String jarFilename;
    File outputFile;
    String locationFactory;
    String id;
    LocationFactory lf;

    CommandLineParser parser = new GnuParser();
    Options options = new Options();
    options.addOption("i", "id", true, "Account ID");
    options.addOption("j", "jar", true, "Application JAR");
    options.addOption("f", "locfactory", true, "Location Factory (LOCAL or DISTRIBUTED)");
    options.addOption("o", "output", true, "Output");

    // Check all the options of command line
    try {
      CommandLine line = parser.parse(options, args);
      if (!line.hasOption("jar")) {
        LOG.error("Application JAR not specified.");
        return -1;
      }
      if (!line.hasOption("output")) {
        LOG.error("Output file not specified.");
        return -1;
      }
      if (!line.hasOption("id")) {
        LOG.error("Account id not specified.");
        return -1;
      }
      if (!line.hasOption("locfactory")) {
        LOG.error("Location factory not specified.");
        return -1;
      }

      jarFilename = line.getOptionValue("jar");
      outputFile = new File(line.getOptionValue("output"));
      id = line.getOptionValue("id");
      locationFactory = line.getOptionValue("locfactory");
    } catch (ParseException e) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("SandboxJVM", options);
      return -1;
    }

    // Load the JAR using the JAR class load and load the manifest file.
    File unpackedJarDir = Files.createTempDir();

    Object app;
    try {
      try {
        if ("LOCAL".equals(locationFactory)) {
          lf = new LocalLocationFactory();
        } else if ("DISTRIBUTED".equals(locationFactory)) {
          lf = new HDFSLocationFactory(new Configuration());
        } else {
          LOG.error("Unknown location factory specified");
          return -1;
        }

        Program archive = Programs.createWithUnpack(lf.create(jarFilename), unpackedJarDir);
        app = archive.getMainClass().newInstance();
      } catch (Exception e) {
        LOG.error(e.getMessage());
        return -1;
      }

      // Now, we are ready to call configure on application.
      // Setup security manager, this setting allows only output file to be written.
      // Nothing else can be done from here on other than creating that file.
      ApplicationSecurity.builder()
        .add(new FilePermission(outputFile.getAbsolutePath(), "write"))
        .apply();
  
      // Now, we call configure, which returns application specification.
      ApplicationSpecification specification;
      if (app instanceof com.continuuity.api.Application) {
        specification = Specifications.from(((com.continuuity.api.Application) app).configure());
      } else if (app instanceof Application) {
        DefaultAppConfigurer configurer = new DefaultAppConfigurer((Application) app);
        ((Application) app).configure(configurer, new ApplicationContext());
        specification = configurer.createApplicationSpec();
      } else {
        throw new IllegalStateException(String.format("Application main class is of invalid type: %s",
                                                      app.getClass().getName()));
      }
      // Convert the specification to JSON.
      // We write the Application specification to output file in JSON format.
      try {
        Writer writer = Files.newWriter(outputFile, Charsets.UTF_8);
        try {
          // TODO: The SchemaGenerator should be injected.
          ApplicationSpecificationAdapter.create(new ReflectionSchemaGenerator()).toJson(specification, writer);
        } finally {
          writer.close();
        }
      } catch (IOException e) {
        LOG.error("Error writing to file {}. {}", outputFile, e.getMessage());
        return -1;
      }
    } finally {
      try {
        FileUtils.deleteDirectory(unpackedJarDir);
      } catch (IOException e) {
        LOG.warn("Error deleting temp unpacked jar directory: {}", unpackedJarDir.getAbsolutePath(), e);
      }
    }

    return 0;
  }

  /**
   * Invoked from command line.
   *
   * @param args specified on command line.
   */
  public static void main(String[] args) {
    SandboxJVM sandboxJVM = new SandboxJVM();
    System.exit(sandboxJVM.doMain(args));
  }
}
