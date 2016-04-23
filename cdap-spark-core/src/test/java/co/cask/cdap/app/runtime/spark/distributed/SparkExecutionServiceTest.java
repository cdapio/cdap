/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.app.runtime.spark.distributed;

import co.cask.cdap.api.workflow.Value;
import co.cask.cdap.common.app.RunIds;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.internal.app.runtime.workflow.BasicWorkflowToken;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.Service;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetAddress;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for {@link SparkExecutionService} and {@link SparkExecutionClient}.
 */
public class SparkExecutionServiceTest {

  @Test
  public void testCompletion() throws Exception {
    ProgramRunId programRunId = new ProgramRunId("ns", "app", ProgramType.SPARK, "test", RunIds.generate().getId());

    // Start a service that no token is supported
    SparkExecutionService service = new SparkExecutionService(InetAddress.getLoopbackAddress().getCanonicalHostName(),
                                                              programRunId, null);
    service.startAndWait();
    SparkExecutionClient client = new SparkExecutionClient(service.getBaseURI(), programRunId);

    // Heartbeats multiple times.
    for (int i = 0; i < 5; i++) {
      Assert.assertNull(client.heartbeat(null));
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Call complete to notify the service it has been stopped
    client.completed(null);
    service.stopAndWait();
  }

  @Test
  public void testExplicitStop() throws Exception {
    ProgramRunId programRunId = new ProgramRunId("ns", "app", ProgramType.SPARK, "test", RunIds.generate().getId());

    // Start a service that no token is supported
    SparkExecutionService service = new SparkExecutionService(InetAddress.getLoopbackAddress().getCanonicalHostName(),
                                                              programRunId, null);
    service.startAndWait();
    final SparkExecutionClient client = new SparkExecutionClient(service.getBaseURI(), programRunId);

    // Heartbeats multiple times.
    for (int i = 0; i < 5; i++) {
      Assert.assertNull(client.heartbeat(null));
      TimeUnit.MILLISECONDS.sleep(50);
    }

    // Stop the program from the service side
    ListenableFuture<Service.State> stopFuture = service.stop();

    // Expect some future heartbeats will receive the STOP command
    Tasks.waitFor(SparkCommand.STOP, new Callable<SparkCommand>() {
      @Override
      public SparkCommand call() throws Exception {
        return client.heartbeat(null);
      }
    }, 10, TimeUnit.SECONDS, 100, TimeUnit.MILLISECONDS);

    // Call complete to notify the service it has been stopped
    client.completed(null);

    // The stop future of the service should be completed after the client.completed call.
    stopFuture.get(5, TimeUnit.SECONDS);
  }

  @Test
  public void testWorkflowToken() throws Exception {
    ProgramRunId programRunId = new ProgramRunId("ns", "app", ProgramType.SPARK, "test", RunIds.generate().getId());

    // Start a service with empty workflow token
    BasicWorkflowToken token = new BasicWorkflowToken(10);
    token.setCurrentNode("spark");
    SparkExecutionService service = new SparkExecutionService(InetAddress.getLoopbackAddress().getCanonicalHostName(),
                                                              programRunId, token);
    service.startAndWait();
    SparkExecutionClient client = new SparkExecutionClient(service.getBaseURI(), programRunId);

    // Update token via heartbeat
    BasicWorkflowToken clientToken = new BasicWorkflowToken(10);
    clientToken.setCurrentNode("spark");

    for (int i = 0; i < 5; i++) {
      clientToken.put("key", "value" + i);
      client.heartbeat(clientToken);

      // The server side token should get updated
      Assert.assertEquals(Value.of("value" + i), token.get("key", "spark"));
    }

    clientToken.put("completed", "true");
    client.completed(clientToken);

    service.stopAndWait();

    // The token on the service side should get updated after the completed call.
    Map<String, Value> values = token.getAllFromNode("spark");
    Map<String, Value> expected = ImmutableMap.of(
      "key", Value.of("value4"),
      "completed", Value.of("true")
    );
    Assert.assertEquals(expected, values);
  }
}
