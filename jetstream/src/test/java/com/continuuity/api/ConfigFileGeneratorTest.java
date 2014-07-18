/*
 * Copyright 2014 Continuuity, Inc.
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

package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractInputFlowlet;
import com.continuuity.jetstream.flowlet.ConfigFileGenerator;
import com.continuuity.jetstream.flowlet.ConfigFileLocalizer;
import com.continuuity.jetstream.flowlet.InputFlowletSpecification;
import com.continuuity.jetstream.internal.DefaultInputFlowletConfigurer;
import com.continuuity.jetstream.internal.LocalConfigFileGenerator;
import com.continuuity.jetstream.internal.LocalConfigFileLocalizer;
import com.continuuity.jetstream.util.Platform;
import com.google.common.io.ByteStreams;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;

/**
 * Tests the successful creation of Config Files.
 */
public class ConfigFileGeneratorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  private static LocationFactory locationFactory;

  @BeforeClass
  public static void init() throws IOException {
    locationFactory = new LocalLocationFactory(TEMP_FOLDER.newFolder());
  }

  @Test
  public void testBasicFlowletConfig() throws IOException, InterruptedException {
    AbstractInputFlowlet flowlet = new InputFlowletBasic();
    DefaultInputFlowletConfigurer configurer = new DefaultInputFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    InputFlowletSpecification spec = configurer.createInputFlowletSpec();
    ConfigFileGenerator configFileGenerator = new LocalConfigFileGenerator();

    // Create base dir
    Location baseDir = locationFactory.create(TEMP_FOLDER.newFolder().toURI());
    baseDir.mkdirs();

    //Get the library zip, copy it to temp dir, unzip it
    String libFile = Platform.libraryResource();
    Location libZip = baseDir.append(libFile);
    libZip.createNew();
    copyResourceFileToDir(libFile, libZip);
    unzipFile(libZip);

    //Create directory structure to place the GS Config Files
    Location workDir = baseDir.append("work");
    workDir.mkdirs();
    Location queryDir = workDir.append("query");
    queryDir.mkdirs();


    //Create the GS config files
    ConfigFileLocalizer configFileLocalizer = new LocalConfigFileLocalizer(queryDir, configFileGenerator);
    configFileLocalizer.localizeConfigFiles(spec);

    //Create GS binaries - and get the status of generation (if the generation was ok or not)
    StringBuilder errorMsg = new StringBuilder();
    boolean testExitValue = generateBinaries(queryDir, errorMsg);
    Assert.assertTrue(errorMsg.toString(), testExitValue);
  }

  private void copyResourceFileToDir(String fileName, Location libZip) throws IOException {
    InputStream ifres = this.getClass().getClassLoader().getResourceAsStream(fileName);
    OutputStream outputStream = libZip.getOutputStream();
    ByteStreams.copy(ifres, outputStream);
    ifres.close();
    outputStream.close();
  }

  private void unzipFile(Location libZip) throws IOException, InterruptedException {
    String path = libZip.toURI().getPath();
    String dir = getParent(libZip).toURI().getPath();
    Runtime rt = Runtime.getRuntime();
    String cmd = String.format("unzip %s -d %s", path, dir);
    Process unzip = rt.exec(cmd);
    Assert.assertEquals(unzip.waitFor(), 0);
  }

  public static Location getParent(Location location) {
    URI source = location.toURI();

    // If it is root, return null
    if ("/".equals(source.getPath())) {
      return null;
    }

    URI resolvedParent = URI.create(source.toString() + "/..").normalize();
    return location.getLocationFactory().create(resolvedParent);
  }

  private boolean generateBinaries(Location configLocation, StringBuilder errorMsg)
    throws IOException, InterruptedException {
    String[] command = {"bash", "-c", "../../bin/buildit"};
    ProcessBuilder genGS = new ProcessBuilder(command);
    File processWorkingDir = new File(configLocation.toURI().getPath());
    genGS.directory(processWorkingDir);
    //Capture both stdout and stderr
    genGS.redirectErrorStream(true);
    Process p = genGS.start();
    InputStream iStream = p.getInputStream();
    InputStreamReader iReader = new InputStreamReader(iStream);
    BufferedReader b = new BufferedReader(iReader);
    String line;
    while ((line = b.readLine()) != null) {
      //Capture the error msg if the compilation was not successful
      if (line.startsWith("ERROR")) {
        errorMsg.append(line);
      }
    }
    //Returns query compilation's success condition
    return p.waitFor() == 0;
  }
}
