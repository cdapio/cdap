package com.continuuity.common.service.distributed;

import com.continuuity.common.service.distributed.yarn.ApplicationMasterServiceImpl;
import org.junit.Test;

/**
 *
 */
public class AllYARNTest extends YARNTestBase {

  @Test
  public void simple() throws Exception {
    TaskSpecification cgs = new TaskSpecification.Builder(getConfiguration())
      .setPriority(0)
      .setMemory(1024)
      .setNumInstances(2)
      .addCommand("ls -ltr")
      .create();

    ApplicationMasterSpecification ams = new ApplicationMasterSpecification.Builder()
      .addContainerGroupSpecification(cgs)
      .addConfiguration(getConfiguration())
      .create();

    ApplicationMasterService appMasterService = new ApplicationMasterServiceImpl(ams);
    appMasterService.start();
    Thread.sleep(1000);

  }
}
