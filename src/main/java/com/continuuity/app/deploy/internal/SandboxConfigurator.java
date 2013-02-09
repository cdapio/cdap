/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy.internal;

import com.continuuity.app.deploy.Configurator;
import org.hsqldb.lib.StringInputStream;

import java.io.IOException;
import java.io.InputStream;

/**
 * Quarantined configuration generates the configuration of an {@link com.continuuity.api.ApplicationSpecification}
 * by running it outside of the server JVM.
 */
public class SandboxConfigurator implements Configurator {
  private final String jarFilename;
  private final long timeout;
  private Process process;
  private boolean destoryed = false;

  public SandboxConfigurator(String jarFilename, long timeout) {
    this.jarFilename = jarFilename;
    this.timeout = timeout;
  }

  @Override
  public Response call() {
    try {
      process = Runtime.getRuntime().exec(getCommand());
      process.waitFor(); // Waits till the application is finished running.
      Integer exit = process.exitValue();
      if(exit == 0)  {
        // Success, Read the output file construct a response and return.
        return new Response() {
          @Override
          public InputStream get() {
            return new StringInputStream(jarFilename);
          }
        };
      } else {
        // Failed.
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (IOException e) {
      return null;
    }
    return null;
  }

  private String getCommand() {
    return "java -cp something.jar com.continuuity.app.deploy.SandboxJVM myapp.jar json.txt";
  }

  @Override
  public void destory() {
    if(! destoryed) {
      // Kill the process.
      process.destroy();
      // Delete the file
    }
  }
}
