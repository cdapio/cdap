/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.k8s.runtime;

import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.RuntimeSpecification;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.locks.Lock;

/**
 * An implementation of {@link TwillContext} for k8s environment.
 */
public class KubeTwillContext implements TwillContext {

  private final RuntimeSpecification runtimeSpec;
  private final RunId appRunId;
  private final RunId runId;
  private final String[] appArguments;
  private final String[] arguments;
  private final DiscoveryService discoveryService;
  private final DiscoveryServiceClient discoveryServiceClient;

  public KubeTwillContext(RuntimeSpecification runtimeSpec, RunId appRunId, RunId runId,
                          String[] appArguments, String[] arguments,
                          DiscoveryService discoveryService, DiscoveryServiceClient discoveryServiceClient) {
    this.runtimeSpec = runtimeSpec;
    this.appRunId = appRunId;
    this.runId = runId;
    this.appArguments = appArguments;
    this.arguments = arguments;
    this.discoveryService = discoveryService;
    this.discoveryServiceClient = discoveryServiceClient;
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public RunId getApplicationRunId() {
    return appRunId;
  }

  @Override
  public int getInstanceCount() {
    return 1;
  }

  @Override
  public InetAddress getHost() {
    try {
      return InetAddress.getLocalHost();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String[] getArguments() {
    return arguments;
  }

  @Override
  public String[] getApplicationArguments() {
    return appArguments;
  }

  @Override
  public TwillRunnableSpecification getSpecification() {
    return runtimeSpec.getRunnableSpecification();
  }

  @Override
  public int getInstanceId() {
    return 0;
  }

  @Override
  public int getVirtualCores() {
    return runtimeSpec.getResourceSpecification().getVirtualCores();
  }

  @Override
  public int getMaxMemoryMB() {
    return runtimeSpec.getResourceSpecification().getMemorySize();
  }

  @Override
  public ServiceDiscovered discover(String name) {
    return discoveryServiceClient.discover(name);
  }

  @Override
  public Cancellable electLeader(String name, ElectionHandler participantHandler) {
    throw new UnsupportedOperationException("Leader election is not supported");
  }

  @Override
  public Lock createLock(String name) {
    throw new UnsupportedOperationException("Distributed lock is not supported");
  }

  @Override
  public Cancellable announce(String serviceName, int port) {
    return discoveryService.register(new Discoverable(serviceName, new InetSocketAddress(getHost(), port)));
  }

  @Override
  public Cancellable announce(String serviceName, int port, byte[] payload) {
    return discoveryService.register(new Discoverable(serviceName, new InetSocketAddress(getHost(), port), payload));
  }
}
