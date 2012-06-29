package com.continuuity.common.service.distributed;

import com.continuuity.common.service.distributed.yarn.ApplicationManagerServiceImpl;
import org.junit.Test;

/**
 *
 */
public class AllYARNTest extends YARNTestBase {

  @Test
  public void simple() throws Exception {
    ContainerGroupSpecification cgs = new ContainerGroupSpecification.Builder(getConfiguration())
      .setPriority(0)
      .setMemory(1024)
      .setNumInstances(2)
      .addCommand("ls -ltr")
      .create();

    ApplicationMasterSpecification ams = new ApplicationMasterSpecification.Builder()
      .addContainerGroupSpecification(cgs)
      .addConfiguration(getConfiguration())
      .create();

    ApplicationMasterService appMasterService = new ApplicationManagerServiceImpl(ams);
    appMasterService.start();
    Thread.sleep(1000);

  }
}
