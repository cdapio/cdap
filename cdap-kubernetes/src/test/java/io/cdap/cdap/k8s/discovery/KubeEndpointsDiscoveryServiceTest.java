/*
 * Copyright © 2026 Cask Data, Inc.
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

import com.google.common.collect.ImmutableSet;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.kubernetes.client.openapi.models.CoreV1EndpointPort;
import io.kubernetes.client.openapi.models.V1EndpointAddress;
import io.kubernetes.client.openapi.models.V1EndpointSubset;
import io.kubernetes.client.openapi.models.V1Endpoints;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link KubeEndpointsDiscoveryService}.
 */
public class KubeEndpointsDiscoveryServiceTest {

  private static final ApiClientFactory NO_CLUSTER_API_CLIENT_FACTORY = () -> {
    throw new IOException("No Kubernetes cluster in unit test");
  };
  private static final String NAME_PREFIX = "cdap-";
  private static final String TASK_WORKER = "task.worker";
  private static final int PORT = 11020;

  @Test
  public void testToDiscoverablesReturnsEmptySetForMissingEndpointsOrSubsets() {
    Assert.assertTrue(KubeEndpointsDiscoveryService.toDiscoverables(TASK_WORKER, null).isEmpty());
    // When every backing pod goes away, Kubernetes keeps the Endpoints object and drops its
    // subsets rather than deleting it. An empty set is the only signal that nothing is left.
    Assert.assertTrue(KubeEndpointsDiscoveryService.toDiscoverables(TASK_WORKER,
        new V1Endpoints().metadata(taskWorkerMetadata())).isEmpty());
  }

  @Test
  public void testToDiscoverablesSkipsSubsetWithoutReadyAddressesOrPorts() {
    // Pods that aren't ready are in notReadyAddresses, which leaves addresses null.
    V1Endpoints endpoints = new V1Endpoints()
        .metadata(taskWorkerMetadata())
        .addSubsetsItem(new V1EndpointSubset()
            .notReadyAddresses(Collections.singletonList(new V1EndpointAddress().ip("10.0.0.9")))
            .ports(Collections.singletonList(new CoreV1EndpointPort().port(PORT))))
        .addSubsetsItem(new V1EndpointSubset()
            .addresses(Collections.singletonList(new V1EndpointAddress().ip("10.0.0.8"))));

    Assert.assertTrue(KubeEndpointsDiscoveryService.toDiscoverables(TASK_WORKER, endpoints)
        .isEmpty());
  }

  @Test
  public void testToDiscoverablesReturnsOneDiscoverablePerReadyPod() {
    V1Endpoints endpoints = new V1Endpoints()
        .metadata(taskWorkerMetadata())
        .addSubsetsItem(new V1EndpointSubset()
            .addresses(Arrays.asList(
                new V1EndpointAddress().ip("10.0.0.1"),
                new V1EndpointAddress().ip("10.0.0.2")))
            .ports(Collections.singletonList(new CoreV1EndpointPort().port(PORT))));

    Set<Discoverable> discoverables =
        KubeEndpointsDiscoveryService.toDiscoverables(TASK_WORKER, endpoints);

    Assert.assertEquals(ImmutableSet.of("10.0.0.1", "10.0.0.2"), hosts(discoverables));
    for (Discoverable discoverable : discoverables) {
      Assert.assertEquals(TASK_WORKER, discoverable.getName());
      Assert.assertEquals(PORT, discoverable.getSocketAddress().getPort());
      // No node name on the address, so the payload stays empty.
      Assert.assertArrayEquals(new byte[0], discoverable.getPayload());
    }
  }

  @Test
  public void testToDiscoverablesCombinesSubsets() {
    V1Endpoints endpoints = new V1Endpoints()
        .metadata(taskWorkerMetadata())
        .addSubsetsItem(new V1EndpointSubset()
            .addresses(Collections.singletonList(new V1EndpointAddress().ip("10.0.0.1")))
            .ports(Collections.singletonList(new CoreV1EndpointPort().port(PORT))))
        .addSubsetsItem(new V1EndpointSubset()
            .addresses(Collections.singletonList(new V1EndpointAddress().ip("10.0.0.2")))
            .ports(Collections.singletonList(new CoreV1EndpointPort().port(PORT + 1))));

    Set<String> hostPorts = KubeEndpointsDiscoveryService.toDiscoverables(TASK_WORKER, endpoints)
        .stream()
        .map(d -> d.getSocketAddress().getHostString() + ":" + d.getSocketAddress().getPort())
        .collect(Collectors.toSet());

    Assert.assertEquals(ImmutableSet.of("10.0.0.1:" + PORT, "10.0.0.2:" + (PORT + 1)), hostPorts);
  }

  @Test
  public void testToDiscoverablesCarriesNodeNameInPayload() {
    V1Endpoints endpoints = new V1Endpoints()
        .metadata(taskWorkerMetadata())
        .addSubsetsItem(new V1EndpointSubset()
            .addresses(Arrays.asList(
                new V1EndpointAddress().ip("10.0.0.1").nodeName("node-a"),
                new V1EndpointAddress().ip("10.0.0.2")))
            .ports(Collections.singletonList(new CoreV1EndpointPort().port(PORT))));

    Map<String, String> nodeByIp = new HashMap<>();
    for (Discoverable d : KubeEndpointsDiscoveryService.toDiscoverables(TASK_WORKER, endpoints)) {
      nodeByIp.put(d.getSocketAddress().getHostString(),
          new String(d.getPayload(), StandardCharsets.UTF_8));
    }

    Assert.assertEquals("node-a", nodeByIp.get("10.0.0.1"));
    Assert.assertEquals("", nodeByIp.get("10.0.0.2"));
  }

  @Test
  public void testToDiscoverablesSkipsPortWithoutNumber() {
    V1Endpoints endpoints = new V1Endpoints()
        .metadata(taskWorkerMetadata())
        .addSubsetsItem(new V1EndpointSubset()
            .addresses(Collections.singletonList(new V1EndpointAddress().ip("10.0.0.1")))
            .ports(Arrays.asList(
                new CoreV1EndpointPort().name("unnumbered"),
                new CoreV1EndpointPort().port(PORT))));

    Set<Discoverable> discoverables =
        KubeEndpointsDiscoveryService.toDiscoverables(TASK_WORKER, endpoints);

    Assert.assertEquals(1, discoverables.size());
    Assert.assertEquals(PORT, discoverables.iterator().next().getSocketAddress().getPort());
  }

  @Test
  public void testDiscoverReturnsSameServiceDiscoveredAndStartsOneWatcher() {
    try (KubeEndpointsDiscoveryService service = createService()) {
      Assert.assertNull("No watcher before the first discover", service.getWatcherThread());

      ServiceDiscovered first = service.discover(TASK_WORKER);
      AbstractDiscoveryWatcherThread<V1Endpoints> watcher = service.getWatcherThread();
      ServiceDiscovered second = service.discover(TASK_WORKER);

      Assert.assertSame(first, second);
      Assert.assertNotNull(watcher);
      Assert.assertSame(watcher, service.getWatcherThread());
      Assert.assertTrue(watcher.isDaemon());
    }
  }

  @Test
  public void testWatcherEventsUpdateServiceDiscovered() {
    try (KubeEndpointsDiscoveryService service = createService()) {
      ServiceDiscovered discovered = service.discover(TASK_WORKER);
      AbstractDiscoveryWatcherThread<V1Endpoints> watcher = service.getWatcherThread();

      V1Endpoints withTwoPods = new V1Endpoints()
          .metadata(taskWorkerMetadata())
          .addSubsetsItem(new V1EndpointSubset()
              .addresses(Arrays.asList(
                  new V1EndpointAddress().ip("10.0.0.1").nodeName("node-a"),
                  new V1EndpointAddress().ip("10.0.0.2").nodeName("node-b")))
              .ports(Collections.singletonList(new CoreV1EndpointPort().port(PORT))));
      watcher.resourceAdded(withTwoPods);
      Assert.assertEquals(ImmutableSet.of("10.0.0.1", "10.0.0.2"), hosts(discovered));

      // Dropping all subsets (every backing pod gone) publishes an empty set.
      watcher.resourceModified(new V1Endpoints().metadata(taskWorkerMetadata()));
      Assert.assertTrue(hosts(discovered).isEmpty());

      // Deleting the Endpoints object also clears the addresses.
      watcher.resourceAdded(withTwoPods);
      Assert.assertFalse(hosts(discovered).isEmpty());
      watcher.resourceDeleted(withTwoPods);
      Assert.assertTrue(hosts(discovered).isEmpty());
    }
  }

  @Test
  public void testCloseStopsTheWatcher() throws InterruptedException {
    KubeEndpointsDiscoveryService service = createService();
    service.discover(TASK_WORKER);
    AbstractDiscoveryWatcherThread<V1Endpoints> watcher = service.getWatcherThread();

    service.close();

    watcher.join(10000);
    Assert.assertFalse(watcher.isAlive());
    Assert.assertNull(service.getWatcherThread());
  }

  @Test
  public void testCloseBeforeDiscoverIsAllowed() {
    createService().close();
  }

  @Test(expected = IllegalStateException.class)
  public void testDiscoverAfterCloseThrows() {
    KubeEndpointsDiscoveryService service = createService();
    service.close();
    service.discover(TASK_WORKER);
  }

  private static KubeEndpointsDiscoveryService createService() {
    return new KubeEndpointsDiscoveryService("default", NAME_PREFIX,
        NO_CLUSTER_API_CLIENT_FACTORY);
  }

  private static V1ObjectMeta taskWorkerMetadata() {
    // Kubernetes copies the Service labels onto the Endpoints object it manages, but not the
    // annotations, so the cdap.service.payload annotation is deliberately absent.
    return new V1ObjectMeta()
        .name(NAME_PREFIX + "task-worker")
        .labels(Collections.singletonMap(KubeDiscoveryService.SERVICE_LABEL,
            NAME_PREFIX + TASK_WORKER));
  }

  private static Set<String> hosts(Iterable<Discoverable> discoverables) {
    return StreamSupport.stream(discoverables.spliterator(), false)
        .map(d -> d.getSocketAddress().getHostString())
        .collect(Collectors.toSet());
  }
}
