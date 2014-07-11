package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractGSFlowlet;
import com.continuuity.jetstream.gsflowlet.GSConfigFileGenerator;
import com.continuuity.jetstream.gsflowlet.GSConfigFileLocalizer;
import com.continuuity.jetstream.gsflowlet.GSFlowletSpecification;
import com.continuuity.jetstream.internal.DefaultGSFlowletConfigurer;
import com.continuuity.jetstream.internal.LocalGSConfigFileGenerator;
import com.continuuity.jetstream.internal.LocalGSConfigFileLocalizer;
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
 * Tests the successful creation of GS Config Files.
 */
public class GSConfigFileGeneratorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testBasicGSFlowletConfig() throws IOException, InterruptedException {
    AbstractGSFlowlet flowlet = new GSFlowletBasic();
    DefaultGSFlowletConfigurer configurer = new DefaultGSFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    GSFlowletSpecification spec = configurer.createGSFlowletSpec();
    GSConfigFileGenerator configFileGenerator = new LocalGSConfigFileGenerator();
    File tempDir = TEMP_FOLDER.newFolder();
    tempDir.mkdir();

    //Get the library zip, copy it to temp dir, unzip it
    String libFile = Platform.gsLibraryResource();
    File libZip = new File(tempDir, libFile);
    copyResourceFileToDir(libFile, libZip);
    unzipFile(libZip);

    //Create directory structure to place the GS Config Files
    File workDir = new File(tempDir, "work");
    workDir.mkdir();
    File queryDir = new File(workDir, "query");
    queryDir.mkdir();

    //Create the GS config files
    GSConfigFileLocalizer configFileLocalizer = new LocalGSConfigFileLocalizer(queryDir, configFileGenerator);
    configFileLocalizer.localizeGSConfigFiles(spec);

    //Create GS binaries - and get the status of generation (if the generation was ok or not)
    StringBuilder errorMsg = new StringBuilder();
    boolean testExitValue = generateGSProcesses(queryDir, errorMsg);
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

  private boolean generateGSProcesses(File configLocation, StringBuilder errorMsg) throws IOException {
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
    return p.exitValue() == 0;
  }
}
