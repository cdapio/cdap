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

package com.continuuity.api.service;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Service;
import org.apache.twill.api.Command;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunnableSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link org.apache.twill.api.TwillRunnable} that accepts a {@link com.google.common.util.concurrent.Service} and
 * runs it as a Twill application.
 */
public class GuavaServiceTwillRunnable implements TwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(GuavaServiceTwillRunnable.class);
  private Service service;
  private String name;
  private ConcurrentHashMap<String, String> runnableArgs;
  private ClassLoader programClassLoader;

  /**
   * Create an {@link com.continuuity.api.service.GuavaServiceTwillRunnable} from a {@link com.google.common.util.concurrent.Service}
   * @param name Name of runnable.
   * @param service Guava service to be run.
   * @param runnableArgs Arguments for the runnable.
   */
  public GuavaServiceTwillRunnable(String name, Service service, Map<String, String> runnableArgs) {
    this.service = service;
    this.runnableArgs = new ConcurrentHashMap<String, String>(runnableArgs);
    this.name = name;
  }

  /**
   * Utility constructor used to instantiate the service from the program classloader.
   * @param programClassLoader classloader to instantiate the service with.
   */
  public GuavaServiceTwillRunnable(ClassLoader programClassLoader) {
    this.programClassLoader = programClassLoader;
  }

  @Override
  public TwillRunnableSpecification configure() {
    runnableArgs.put("service.class.name", service.getClass().getName());
    runnableArgs.put("service.runnable.name", name);
    return TwillRunnableSpecification.Builder.with()
      .setName(name)
      .withConfigs(ImmutableMap.copyOf(runnableArgs))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    runnableArgs = new ConcurrentHashMap<String, String>(context.getSpecification().getConfigs());
    String serviceClassName = runnableArgs.remove("service.class.name");
    name = runnableArgs.remove("service.runnable.name");

    try {
      Class<?> serviceClass = programClassLoader.loadClass(serviceClassName);
      service = (Service) serviceClass.newInstance();
    } catch (Exception e) {
      LOG.error("Could not instantiate service " + name);
      Throwables.propagate(e);
    }

    service.startAndWait();
    LOG.info("Instantiated service " + name);
    context.announce(service.getClass().getName(), getRandomPort());
  }

  @Override
  public void handleCommand(Command command) throws Exception {

  }

  @Override
  public void stop() {
    LOG.info("Stopping service " + name);
    service.stopAndWait();
  }

  @Override
  public void destroy() {
    // no-op
  }

  @Override
  public void run() {
    // no-op
  }

  private int getRandomPort() {
    try {
      ServerSocket socket = new ServerSocket(0);
      try {
        return socket.getLocalPort();
      } finally {
        socket.close();
      }
    } catch (IOException e) {
      return -1;
    }
  }
}
