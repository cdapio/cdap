/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.runtime.spi.runtimejob;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import org.apache.twill.filesystem.FileContextLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.internal.zookeeper.InMemoryZKServer;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for {@link DataprocRuntimeEnvironment}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({
  DataprocRuntimeEnvironment.class,
  InMemoryZKServer.class,
  YarnTwillRunnerService.class
})
@PowerMockIgnore({
  "javax.management.*",
  "javax.xml.*",
  "org.xml.*",
  "org.w3c.*",
  "com.sun.org.apache.xerces.*",
  "jdk.xml.*",
  "ch.qos.logback.*",
  "org.slf4j.*"
})
public class DataprocRuntimeEnvironmentTest {

  @Test
  public void testInitializeAndDestroyLifecycle() throws Exception {
    InMemoryZKServer.Builder mockBuilder = Mockito.mock(InMemoryZKServer.Builder.class);
    InMemoryZKServer mockZkServer = Mockito.mock(InMemoryZKServer.class);
    Mockito.when(mockBuilder.build()).thenReturn(mockZkServer);
    Mockito.when(mockZkServer.startAsync()).thenReturn(mockZkServer);
    Mockito.when(mockZkServer.stopAsync()).thenReturn(mockZkServer);
    Mockito.when(mockZkServer.getLocalAddress())
        .thenReturn(new InetSocketAddress("127.0.0.1", 21819));

    PowerMockito.mockStatic(InMemoryZKServer.class);
    Mockito.when(InMemoryZKServer.builder()).thenReturn(mockBuilder);

    FileContextLocationFactory mockLocationFactory =
        Mockito.mock(FileContextLocationFactory.class);
    Location mockRootLocation = Mockito.mock(Location.class);
    Mockito.when(mockLocationFactory.create("/")).thenReturn(mockRootLocation);
    PowerMockito.whenNew(FileContextLocationFactory.class)
        .withAnyArguments()
        .thenReturn(mockLocationFactory);

    YarnTwillRunnerService mockTwillRunner = PowerMockito.mock(YarnTwillRunnerService.class);
    PowerMockito.whenNew(YarnTwillRunnerService.class)
        .withAnyArguments()
        .thenReturn(mockTwillRunner);

    DataprocRuntimeEnvironment env = new DataprocRuntimeEnvironment();
    env.initialize("spark3_2.12", "CLIENT");

    Mockito.verify(mockZkServer).startAsync();
    Mockito.verify(mockZkServer).awaitRunning();
    Mockito.verify(mockTwillRunner).start();
    Assert.assertEquals(LaunchMode.CLIENT, env.getLaunchMode());
    Assert.assertSame(mockLocationFactory, env.getLocationFactory());
    Assert.assertSame(mockTwillRunner, env.getTwillRunner());

    Map<String, String> props = env.getProperties();
    Assert.assertEquals("spark3_2.12", props.get("app.program.spark.compat"));
    Assert.assertEquals("127.0.0.1:21819", props.get("zookeeper.quorum"));

    // Verify destroy() stops runner and ZK server and swallows IOException from root cleanup
    Mockito.doThrow(new IOException("simulated delete failure"))
        .when(mockRootLocation).delete(true);
    env.destroy();

    Mockito.verify(mockTwillRunner).stop();
    Mockito.verify(mockZkServer).stopAsync();
    Mockito.verify(mockZkServer).awaitTerminated();
    Mockito.verify(mockLocationFactory).create("/");
    Mockito.verify(mockRootLocation).delete(true);
  }
}
