/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.common.test;

import co.cask.cdap.common.app.RunIds;
import com.google.common.base.Throwables;
import org.apache.twill.api.ElectionHandler;
import org.apache.twill.api.RunId;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnableSpecification;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.locks.Lock;

/**
 * A mock {@link TwillContext} for unit-test.
 */
public final class MockTwillContext implements TwillContext {

  private final RunId runId = RunIds.generate();
  private final RunId appRunId = RunIds.generate();

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
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String[] getArguments() {
    return new String[0];
  }

  @Override
  public String[] getApplicationArguments() {
    return new String[0];
  }

  @Override
  public TwillRunnableSpecification getSpecification() {
    return null;
  }

  @Override
  public int getInstanceId() {
    return 0;
  }

  @Override
  public int getVirtualCores() {
    return 0;
  }

  @Override
  public int getMaxMemoryMB() {
    return 0;
  }

  @Override
  public ServiceDiscovered discover(String name) {
    return null;
  }

  @Override
  public Cancellable electLeader(String name, ElectionHandler participantHandler) {
    return null;
  }

  @Override
  public Lock createLock(String name) {
    return null;
  }

  @Override
  public Cancellable announce(String serviceName, int port) {
    return null;
  }

  @Override
  public Cancellable announce(String serviceName, int port, byte[] payload) {
    return null;
  }
}
