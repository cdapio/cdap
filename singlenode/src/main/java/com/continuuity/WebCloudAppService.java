package com.continuuity;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.service.Server;
import com.continuuity.common.service.ServerException;

import java.io.IOException;

public class WebCloudAppService implements Server {

  // This is the external process that will wrap the web app
  Process webAppProcess;

  @Override
  public void start(String[] args, CConfiguration conf) throws ServerException {

    // Create a new Process
    ProcessBuilder builder =
      new ProcessBuilder("../web-cloud-app/bin/node",
        "../web-cloud-app/build/server/main.js");

    try {
      webAppProcess = builder.start();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void stop(boolean now) throws ServerException {
    webAppProcess.destroy();
  }

}
