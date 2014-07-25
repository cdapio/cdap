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
 *
 */
public class GuavaServiceTwillRunnable implements TwillRunnable {
  private static final Logger LOG = LoggerFactory.getLogger(GuavaServiceTwillRunnable.class);
  private Service service;
  private ConcurrentHashMap<String, String> args;
  private ClassLoader classLoader;

  public GuavaServiceTwillRunnable(Service service, Map<String, String> args) {
    this.service = service;
    this.args = new ConcurrentHashMap<String, String>(args);
  }

  public GuavaServiceTwillRunnable(ClassLoader classLoader) {
    this.classLoader = classLoader;
  }

  @Override
  public TwillRunnableSpecification configure() {
    args.put("service.class.name", service.getClass().getName());
    return TwillRunnableSpecification.Builder.with()
      .setName(service.getClass().getSimpleName())
      .withConfigs(ImmutableMap.copyOf(args))
      .build();
  }

  @Override
  public void initialize(TwillContext context) {
    args = new ConcurrentHashMap<String, String>(context.getSpecification().getConfigs());
    String serviceClassName = args.remove("service.class.name");
    LOG.info(serviceClassName);
    try {
      Class<?> serviceClass = classLoader.loadClass(serviceClassName);
      service = (Service) serviceClass.newInstance();
    } catch (Exception e) {
      LOG.error("Could not instantiate service " + serviceClassName);
      Throwables.propagate(e);
    }

    LOG.info("Instantiated " + serviceClassName);
    service.startAndWait();
    context.announce(service.getClass().getName(), getRandomPort());
  }

  @Override
  public void handleCommand(Command command) throws Exception {

  }

  @Override
  public void stop() {
    service.stopAndWait();
  }

  @Override
  public void destroy() {

  }

  @Override
  public void run() {

  }

  public int getRandomPort() {
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
