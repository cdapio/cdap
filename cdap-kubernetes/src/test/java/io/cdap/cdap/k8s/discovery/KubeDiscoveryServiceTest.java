/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.cdap.k8s.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.environment.k8s.DefaultApiClientFactory;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Test for {@link KubeDiscoveryService}. The test is disabled by default since it requires a running
 * kubernetes cluster for the test to run. This class is kept for development purpose.
 */
@Ignore
public class KubeDiscoveryServiceTest {
  private static final ApiClientFactory API_CLIENT_FACTORY = new DefaultApiClientFactory(10, 300);

  @Test
  public void testDiscoveryService() throws Exception {
    String namespace = "default";
    Map<String, String> podLabels = ImmutableMap.of("cdap.container", "test");
    try (KubeDiscoveryService service = new KubeDiscoveryService(namespace, "cdap-test-",
                                                                 podLabels, Collections.emptyList(),
                                                                 API_CLIENT_FACTORY)) {
      // Watch for changes
      ServiceDiscovered serviceDiscovered = service.discover("test.service");

      BlockingQueue<Set<Discoverable>> queue = new LinkedBlockingQueue<>();
      serviceDiscovered.watchChanges(sd -> {
        // Update to the latest
        queue.add(StreamSupport.stream(sd.spliterator(), false).collect(Collectors.toSet()));
      }, MoreExecutors.directExecutor());

      // Initially there should be no such service
      Set<Discoverable> discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertTrue(discoverables.isEmpty());

      // Then, register the service
      service.register(new Discoverable("test.service",
                                        new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234),
                                        "https".getBytes(StandardCharsets.UTF_8)));

      // Now there should be a new discoverable
      discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertEquals(1, discoverables.size());

      // The discoverable should have hostname the same as the k8s service name and port from the discoverable
      Discoverable discoverable = discoverables.stream().findFirst().orElseThrow(Exception::new);
      Assert.assertEquals(1234, discoverable.getSocketAddress().getPort());
      Assert.assertEquals("https", new String(discoverable.getPayload(), StandardCharsets.UTF_8));
      Assert.assertEquals("cdap-test-test-service." + namespace, discoverable.getSocketAddress().getHostName());

      // Register the service again with different port. This is to simulate config update
      service.register(new Discoverable("test.service", new InetSocketAddress(InetAddress.getLoopbackAddress(), 4321)));

      // Should have a new discoverable
      discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertEquals(1, discoverables.size());

      // The discoverable should have hostname the same as the k8s service name and port from the discoverable
      discoverable = discoverables.stream().findFirst().orElseThrow(Exception::new);
      Assert.assertEquals(4321, discoverable.getSocketAddress().getPort());
      Assert.assertArrayEquals(new byte[0], discoverable.getPayload());
      Assert.assertEquals("cdap-test-test-service." + namespace, discoverable.getSocketAddress().getHostName());

      // (CDAP-19415) Add a discovery of a new service. This would force recreating a new watch.
      service.discover("test.service2").watchChanges(sd -> {
        queue.add(StreamSupport.stream(sd.spliterator(), false).collect(Collectors.toSet()));
      }, MoreExecutors.directExecutor());

      // The new service shouldn't be available yet.
      discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertTrue(discoverables.isEmpty());

      // Register the new service.
      service.register(new Discoverable("test.service2",
                                        new InetSocketAddress(InetAddress.getLoopbackAddress(), 6789),
                                        "https".getBytes(StandardCharsets.UTF_8)));

      // We should see the new service being notified.
      discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertEquals(1, discoverables.size());

      discoverable = discoverables.stream().findFirst().orElseThrow(Exception::new);
      Assert.assertEquals(6789, discoverable.getSocketAddress().getPort());
      Assert.assertEquals("https", new String(discoverable.getPayload(), StandardCharsets.UTF_8));
      Assert.assertEquals("cdap-test-test-service2." + namespace, discoverable.getSocketAddress().getHostName());

    } finally {
      // Cleanup the created service
      CoreV1Api api = new CoreV1Api(API_CLIENT_FACTORY.create());
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      api.deleteNamespacedService("cdap-test-test-service", namespace, null, null, null, null, null, deleteOptions);
      api.deleteNamespacedService("cdap-test-test-service2", namespace, null, null, null, null, null, deleteOptions);
    }
  }

  @Test
  public void testCloseWatchRace() throws Exception {
    String namespace = "default";
    String prefix = "cdap-test-";
    try (KubeDiscoveryService service = new KubeDiscoveryService(namespace, prefix,
                                                                 ImmutableMap.of("cdap.container", "test"),
                                                                 Collections.emptyList(), API_CLIENT_FACTORY)) {
      // Register two services first
      service.register(new Discoverable("test1", new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
      service.register(new Discoverable("test2", new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678)));

      // Discover the first service. When receiving a change notification, start watching the second service from the
      // change listener. This would cause a closeWatch() call in the watch thread while in the creation of
      // the watch.
      ServiceDiscovered discovered1 = service.discover("test1");
      CompletableFuture<ServiceDiscovered> completion = new CompletableFuture<>();
      discovered1.watchChanges(sd -> {
        if (sd.iterator().hasNext()) {
          completion.complete(service.discover("test2"));
        }
      }, MoreExecutors.directExecutor());

      // Make sure we see the 2nd service
      CompletableFuture<Discoverable> discoverableCompletion = new CompletableFuture<>();
      completion.get(10, TimeUnit.SECONDS).watchChanges(sd -> {
        for (Discoverable discoverable : sd) {
          discoverableCompletion.complete(discoverable);
          break;
        }
      }, MoreExecutors.directExecutor());

      Discoverable discoverable = discoverableCompletion.get(10, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverable);
      Assert.assertEquals(prefix + "test2." + namespace, discoverable.getSocketAddress().getHostName());
    } finally {
      CoreV1Api api = new CoreV1Api(API_CLIENT_FACTORY.create());
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      api.deleteNamespacedService(prefix + "test1", namespace, null, null, null, null, null, deleteOptions);
      api.deleteNamespacedService(prefix + "test2", namespace, null, null, null, null, null, deleteOptions);
    }
  }

  @Test
  public void testCloseWatchRaceDifferentThreads() throws Exception {
    String namespace = "default";
    String prefix = "cdap-test-";
    try (KubeDiscoveryService service = new KubeDiscoveryService(namespace, prefix,
                                                                 ImmutableMap.of("cdap.container", "test"),
                                                                 Collections.emptyList(), API_CLIENT_FACTORY)) {
      // Register two services first
      service.register(new Discoverable("test1", new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
      service.register(new Discoverable("test2", new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678)));

      // Discover the first service.
      CompletableFuture<Discoverable> completion1 = new CompletableFuture<>();
      service.discover("test1").watchChanges(sd -> {
        for (Discoverable discoverable : sd) {
          completion1.complete(discoverable);
          break;
        }
      }, MoreExecutors.directExecutor());

      // When the first service was discovered, start watching the 2nd service.
      // This is a race between this thread and the watcher thread
      completion1.get(10, TimeUnit.SECONDS);

      // Make sure we see the 2nd service
      CompletableFuture<Discoverable> completion2 = new CompletableFuture<>();
      service.discover("test2").watchChanges(sd -> {
        for (Discoverable discoverable : sd) {
          completion2.complete(discoverable);
          break;
        }
      }, MoreExecutors.directExecutor());

      Discoverable discoverable = completion2.get(10, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverable);
      Assert.assertEquals(prefix + "test2." + namespace, discoverable.getSocketAddress().getHostName());
    } finally {
      CoreV1Api api = new CoreV1Api(API_CLIENT_FACTORY.create());
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      api.deleteNamespacedService(prefix + "test1", namespace, null, null, null, null, null, deleteOptions);
      api.deleteNamespacedService(prefix + "test2", namespace, null, null, null, null, null, deleteOptions);
    }
  }
}
