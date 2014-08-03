/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.internal.app.runtime.service;

import co.cask.cdap.common.election.InMemoryElectionRegistry;
import com.google.common.collect.Sets;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryService;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.discovery.ServiceDiscovered;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Set;

/**
 * Implementation of {@link TwillContext} for the in memory twill runnable runner.
 */
final class InMemoryTwillContext implements TwillContext, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryTwillContext.class);

  private final RunId runId;
  private final RunId applicationRunId;
  private final InetAddress host;
  private final String[] arguments;
  private final String[] applicationArguments;
  private final TwillRunnableSpecification specification;
  private final int instanceId;
  private final int virtualCores;
  private final int maxMemoryMB;
  private final DiscoveryServiceClient discoveryServiceClient;
  private final DiscoveryService discoveryService;
  private final InMemoryElectionRegistry electionRegistry;
  private final Set<Cancellable> electionCancels;

  private volatile int instanceCount;

  InMemoryTwillContext(RunId runId, RunId applicationRunId, InetAddress host, String[] arguments,
                       String[] applicationArguments, TwillRunnableSpecification specification, int instanceId,
                       int virtualCores, int maxMemoryMB, DiscoveryServiceClient discoveryServiceClient,
                       DiscoveryService discoveryService, int instanceCount,
                       InMemoryElectionRegistry electionRegistry) {
    this.runId = runId;
    this.applicationRunId = applicationRunId;
    this.host = host;
    this.arguments = arguments;
    this.applicationArguments = applicationArguments;
    this.specification = specification;
    this.instanceId = instanceId;
    this.virtualCores = virtualCores;
    this.maxMemoryMB = maxMemoryMB;
    this.discoveryServiceClient = discoveryServiceClient;
    this.discoveryService = discoveryService;
    this.instanceCount = instanceCount;
    this.electionRegistry = electionRegistry;
    this.electionCancels = Sets.newCopyOnWriteArraySet();
  }

  @Override
  public RunId getRunId() {
    return runId;
  }

  @Override
  public RunId getApplicationRunId() {
    return applicationRunId;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount;
  }

  public void setInstanceCount(int instanceCount) {
    this.instanceCount = instanceCount;
  }

  @Override
  public InetAddress getHost() {
    return host;
  }

  @Override
  public String[] getArguments() {
    return arguments;
  }

  @Override
  public String[] getApplicationArguments() {
    return applicationArguments;
  }

  @Override
  public TwillRunnableSpecification getSpecification() {
    return specification;
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public int getVirtualCores() {
    return virtualCores;
  }

  @Override
  public int getMaxMemoryMB() {
    return maxMemoryMB;
  }

  @Override
  public ServiceDiscovered discover(String name) {
    return discoveryServiceClient.discover(name);
  }

  @Override
  public Cancellable electLeader(String name, ElectionHandler participantHandler) {
    // Namespace it under the application runId
    String electionName = String.format("%s.%s", applicationRunId.getId(), name);

    final Cancellable delegate = electionRegistry.join(electionName, participantHandler);
    Cancellable cancel = new Cancellable() {
      @Override
      public void cancel() {
        try {
          delegate.cancel();
        } finally {
          electionCancels.remove(this);
        }
      }
    };

    electionCancels.add(cancel);
    return cancel;
  }

  @Override
  public Cancellable announce(final String serviceName, final int port) {
    return discoveryService.register(new Discoverable() {
      @Override
      public String getName() {
        return serviceName;
      }

      @Override
      public InetSocketAddress getSocketAddress() {
        return new InetSocketAddress(host, port);
      }
    });
  }

  @Override
  public void close() throws IOException {
    for (Cancellable c : electionCancels) {
      try {
        c.cancel();
      } catch (Throwable t) {
        LOG.error("Exception when withdraw from leader election.", t);
      }
    }
  }
}
