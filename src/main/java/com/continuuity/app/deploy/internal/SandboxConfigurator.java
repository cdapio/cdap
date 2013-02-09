/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.app.deploy.internal;

import com.continuuity.app.deploy.ConfigResponse;
import com.continuuity.app.deploy.Configurator;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.StringReader;
import java.net.URI;

/**
 * SandboxConfigurator spawns a seperate JVM to run configuration of an Application.
 * <p>
 *   This class is responsible for starting the process of generating configuration
 *   by passing in the JAR file of Application be configured by a seperate JVM.
 * </p>
 */
public class SandboxConfigurator implements Configurator {
  /**
   * Prefix of temporary file.
   */
  private final static String PREFIX = "app-specification";
  /**
   * Extension of temporary.
   */
  private final static String EXT = ".json";

  /**
   * Name of JAR file.
   */
  private final File jarFilename;

  /**
   * Sandbox process.
   */
  private Process process;

  /**
   * Constructor
   * @param jarFilename Name of the JAR file.
   */
  public SandboxConfigurator(File jarFilename) {
    Preconditions.checkNotNull(jarFilename);
    this.jarFilename = jarFilename;
  }

  /**
   * Helper for simplifying creating {@link SandboxConfigurator}
   * @param jarFilename Name of the file.
   * @return An instance of {@link ListenableFuture}
   */
  public static ListenableFuture<ConfigResponse> config(File jarFilename) {
    SandboxConfigurator sc = new SandboxConfigurator(jarFilename);
    return sc.config();
  }

  /**
   * Runs the <code>Application.configure()</code> in a sandbox JVM
   * with high level of security.
   *
   * @return An instance of {@link ListenableFuture}
   */
  @Override
  public ListenableFuture<ConfigResponse> config() {
    final SettableFuture<ConfigResponse> result = SettableFuture.create();
    final File outputFile;

    try {
      outputFile = File.createTempFile(PREFIX, EXT);

      // Run the command in seperate JVM.
      process = Runtime.getRuntime().exec(getCommand(outputFile));

      // Add future to handle the case when the future is cancelled.
      // OnSuccess, we don't do anything, onFailure, we make sure that
      // process is destroyed.
      Futures.addCallback(result, new FutureCallback<ConfigResponse>() {
        @Override
        public void onSuccess(final ConfigResponse result) {
          return; // does nothing.
        }

        @Override
        public void onFailure(final Throwable t) {
          // In case the future was cancelled, we have to
          // destroy the process.
          if(result.isCancelled()) {
            process.destroy();
          }
        }
      });
    } catch(Exception e) {
      // Returns a {@code ListenableFuture} which has an exception set immediately
      // upon construction.
      return Futures.immediateFailedFuture(e);
    }

    // Start a thread that waits for process to complete
    new Thread() {
      @Override
      public void run() {
        try {
          // Wait for process to exit and extract the return. If cancelled the process will
          // be shutdown.
          process.waitFor();
          int exit = process.exitValue();
          if(exit == 0)  {
            result.set(new DefaultConfigResponse(0, new BufferedReader(new FileReader(outputFile))));
          } else {
            result.set(new DefaultConfigResponse(exit, new StringReader("")));
          }
        } catch (Exception e) {
          result.setException(e);
        }
      }
    }.start();

    return result;
  }

  /**
   * @return Returns the command used to execute configure using <code>outputFile</code> in which
   * the output of run would be stored.
   */
  private String getCommand(File outputFile) {
    return String.format("--jar %s --output %s", jarFilename.getAbsolutePath(),
                         outputFile.getAbsolutePath());
  }
}
