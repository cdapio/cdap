/*
 * com.continuuity - Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity;

import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Executor;

/**
 * WebCloudAppService is a basic Server wrapper that launches node.js and our
 * webapp main.js file. It then basically sits there waiting, doing nothing.
 *
 * All output is sent to our Logging service.
 */
public class WebCloudAppService extends AbstractExecutionThreadService {
  private static final Logger LOG = LoggerFactory.getLogger(WebCloudAppService.class);
  private static final String NODE_JS_EXECUTABLE = "node";
  public static final String WEB_APP = "web-app/server/local/main.js"; // Right path passed on command line.
  private final String webAppPath;
  private Process process;
  private BufferedReader bufferedReader;

  public WebCloudAppService() {
    this(WEB_APP);
  }

  public WebCloudAppService(String webAppPath) {
    this.webAppPath = webAppPath;
  }

  /**
   * Start the service.
   */
  @Override
  protected void startUp() throws Exception {
    ProcessBuilder builder = new ProcessBuilder(NODE_JS_EXECUTABLE, webAppPath);
    builder.redirectErrorStream(true);
    LOG.info("Starting Web Cloud App ... (" + webAppPath + ")");
    process = builder.start();
    final InputStream is = process.getInputStream();
    final InputStreamReader isr = new InputStreamReader(is);
    bufferedReader = new BufferedReader(isr);
  }

  /**
   * Processes the output of the command.
   */
  @Override
  protected void run() throws Exception {
    LOG.info("Web Cloud App running ...");
    try {
      String line;
      while ((line = bufferedReader.readLine()) != null) {
        LOG.trace(line);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  /**
   * Returns the {@link Executor} that will be used to run this service.
   */
  @Override
  protected Executor executor() {
    return new Executor() {
      @Override
      public void execute(Runnable command) {
        Thread thread = new Thread(command, getServiceName());
        thread.setDaemon(true);
        thread.start();
      }
    };
  }

  /**
   * Invoked to request the service to stop.
   * <p/>
   * <p>By default this method does nothing.
   */
  @Override
  protected void triggerShutdown() {
    process.destroy();
  }

  /**
   * Stop the service.
   */
  @Override
  protected void shutDown() throws Exception {
    LOG.info("Shutting down Web Cloud App ...");
    process.waitFor();
  }
}
