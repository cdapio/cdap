/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.app.runtime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * AppFabric server main. It can be started as regular Java Application
 * or started using apache commons daemon (jsvc)
 */
public final class AppFabricMain {

  private static final Logger LOG = LoggerFactory.getLogger(AppFabricMain.class);

  /**
   * The main method. Invoked from command line directly. It simply call methods in the same sequence
   * as if the program is started by jsvc.
   */
  public static void main(final String[] args) throws InterruptedException {
    final AppFabricMain appFabricMain = new AppFabricMain();
    final CountDownLatch shutdownLatch = new CountDownLatch(1);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          try {
            appFabricMain.stop();
          } finally {
            try {
              appFabricMain.destroy();
            } finally {
              shutdownLatch.countDown();
            }
          }
        } catch (Throwable t) {
          LOG.error("Exception when shutting down: " + t.getMessage(), t);
        }
      }
    });
    appFabricMain.init(args);
    appFabricMain.start();

    shutdownLatch.await();
  }

  /**
   * Invoked by jsvc to initialize the program.
   */
  public void init(String[] args) {
  }

  /**
   * Invoked by jsvc to start the program.
   */
  public void start() {
  }

  /**
   * Invoked by jsvc to stop the program.
   */
  public void stop() {
  }

  /**
   * Invoked by jsvc for resource cleanup
   */
  public void destroy() {
  }
}
