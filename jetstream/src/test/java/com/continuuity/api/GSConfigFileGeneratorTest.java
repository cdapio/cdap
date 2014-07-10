package com.continuuity.api;

import com.continuuity.jetstream.api.AbstractGSFlowlet;
import com.continuuity.jetstream.gsflowlet.GSConfigFileGenerator;
import com.continuuity.jetstream.gsflowlet.GSConfigFileLocalizer;
import com.continuuity.jetstream.gsflowlet.GSFlowletSpecification;
import com.continuuity.jetstream.internal.DefaultGSFlowletConfigurer;
import com.continuuity.jetstream.internal.LocalGSConfigFileGenerator;
import com.continuuity.jetstream.internal.LocalGSConfigFileLocalizer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 * Tests the successful creation of GS Config Files.
 */
public class GSConfigFileGeneratorTest {

  @ClassRule
  public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

  @Test
  public void testBasicGSFlowletConfig() throws IOException {
    AbstractGSFlowlet flowlet = new GSFlowletBasic();
    DefaultGSFlowletConfigurer configurer = new DefaultGSFlowletConfigurer(flowlet);
    flowlet.create(configurer);
    GSFlowletSpecification spec = configurer.createGSFlowletSpec();
    GSConfigFileGenerator configFileGenerator = new LocalGSConfigFileGenerator();
    File tempDir = TEMP_FOLDER.newFolder();
    GSConfigFileLocalizer configFileLocalizer = new LocalGSConfigFileLocalizer(tempDir, configFileGenerator);
    configFileLocalizer.localizeGSConfigFiles(spec);
    //TODO: Verify the contents of the files. Also invoke translate_fta to make sure binaries are created successfully.
  }

}
