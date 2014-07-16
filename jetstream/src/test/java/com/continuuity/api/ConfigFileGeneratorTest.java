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
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 * Tests the successful creation of Config Files.
 */
public class ConfigFileGeneratorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testBasicFlowletConfig() throws IOException, InterruptedException {
    AbstractInputFlowlet flowlet = new InputFlowletBasic();
    DefaultInputFlowletConfigurer configurer = new DefaultInputFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    InputFlowletSpecification spec = configurer.createInputFlowletSpec();
    ConfigFileGenerator configFileGenerator = new LocalConfigFileGenerator();
    File tempDir = TEMP_FOLDER.newFolder();
    tempDir.mkdir();

    //Get the library zip, copy it to temp dir, unzip it
    String libFile = Platform.libraryResource();
    File libZip = new File(tempDir, libFile);
    copyResourceFileToDir(libFile, libZip);
    unzipFile(libZip);

    //Create directory structure to place the GS Config Files
    File workDir = new File(tempDir, "work");
    workDir.mkdir();
    File queryDir = new File(workDir, "query");
    queryDir.mkdir();

    //Create the GS config files
    ConfigFileLocalizer configFileLocalizer = new LocalConfigFileLocalizer(queryDir, configFileGenerator);
    configFileLocalizer.localizeConfigFiles(spec);

    //Create GS binaries - and get the status of generation (if the generation was ok or not)
    StringBuilder errorMsg = new StringBuilder();
    boolean testExitValue = generateBinaries(queryDir, errorMsg);
    Assert.assertTrue(errorMsg.toString(), testExitValue);
  }

  private void copyResourceFileToDir(String fileName, File libZip) throws IOException {
    InputStream ifres = this.getClass().getClassLoader().getResourceAsStream(fileName);
    OutputStream outputStream = new FileOutputStream(libZip);
    ByteStreams.copy(ifres, outputStream);
    ifres.close();
    outputStream.close();
  }

  private void unzipFile(File libZip) throws IOException, InterruptedException {
    String path = libZip.getAbsolutePath();
    String dir = libZip.getParent();
    Runtime rt = Runtime.getRuntime();
    String cmd = String.format("unzip %s -d %s", path, dir);
    Process unzip = rt.exec(cmd);
    Assert.assertEquals(unzip.waitFor(), 0);
  }

  private boolean generateBinaries(File configLocation, StringBuilder errorMsg)
    throws IOException, InterruptedException {
    String[] command = {"bash", "-c", "../../bin/buildit"};
    ProcessBuilder genGS = new ProcessBuilder(command);
    genGS.directory(configLocation);
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
