/*
 * Copyright © 2019-2023 Cask Data, Inc.
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
import io.kubernetes.client.openapi.models.V1LoadBalancerIngressBuilder;
import io.kubernetes.client.openapi.models.V1LoadBalancerStatus;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1OwnerReferenceBuilder;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceBuilder;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServicePortBuilder;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1ServiceSpecBuilder;
import io.kubernetes.client.openapi.models.V1ServiceStatus;
import io.kubernetes.client.openapi.models.V1ServiceStatusBuilder;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Test for {@link KubeDiscoveryService}. Some tests are disabled by default
 * since they require a running kubernetes cluster for the test to run. Disabled
 * tests are kept for development purpose.
 */
public class KubeDiscoveryServiceTest {

  private static final ApiClientFactory API_CLIENT_FACTORY = new DefaultApiClientFactory(
      10, 300);
  private static final String LOAD_BALANCER_SERVICE_NAME = "ilb-service";
  private static final String CLUSTER_IP_SERVICE_NAME = "my-service";
  private static final String LOAD_BALANCER_IP = "10.0.0.10";
  private static final String TEST_PAYLOAD = "test-payload";
  private static final String NAME_PREFIX = "cdap-";
  private static final int SERVICE_PORT = 80;
  private static final String ENCODED_PAYLOAD = Base64.getEncoder()
      .encodeToString(
          TEST_PAYLOAD.getBytes(
              StandardCharsets.UTF_8));
  private static final Map<String, String> LOAD_BALANCER_ANNOTATIONS =
      Collections.singletonMap("networking.gke.io/load-balancer-type",
          "Internal");
  private static final Map<String, String> POD_LABELS =
      Collections.singletonMap("cdap.container", "test");
  private static final V1OwnerReference OWNER_REFERENCE = new V1OwnerReferenceBuilder()
      .withApiVersion("apps/v1")
      .withKind("StatefulSet")
      .withName("my-statefulset")
      .withUid("abcd-1234")
      .withController(true)
      .withBlockOwnerDeletion(true)
      .build();

  private KubeDiscoveryService kubeDiscoveryService;

  @Before
  public void beforeTest() {
    kubeDiscoveryService = new KubeDiscoveryService(
        "cdap-namespace",
        NAME_PREFIX,
        POD_LABELS,
        Collections.singletonList(OWNER_REFERENCE),
        API_CLIENT_FACTORY,
        Collections.singletonList(LOAD_BALANCER_SERVICE_NAME),
        LOAD_BALANCER_ANNOTATIONS);
  }


  @Test
  @Ignore
  public void testDiscoveryService() throws Exception {
    String namespace = "default";
    Map<String, String> podLabels = ImmutableMap.of("cdap.container", "test");
    try (KubeDiscoveryService service = new KubeDiscoveryService(namespace,
        "cdap-test-",
        podLabels, Collections.emptyList(),
        API_CLIENT_FACTORY)) {
      // Watch for changes
      ServiceDiscovered serviceDiscovered = service.discover("test.service");

      BlockingQueue<Set<Discoverable>> queue = new LinkedBlockingQueue<>();
      serviceDiscovered.watchChanges(sd -> {
        // Update to the latest
        queue.add(StreamSupport.stream(sd.spliterator(), false)
            .collect(Collectors.toSet()));
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
      Discoverable discoverable = discoverables.stream().findFirst()
          .orElseThrow(Exception::new);
      Assert.assertEquals(1234, discoverable.getSocketAddress().getPort());
      Assert.assertEquals("https",
          new String(discoverable.getPayload(), StandardCharsets.UTF_8));
      Assert.assertEquals("cdap-test-test-service." + namespace,
          discoverable.getSocketAddress().getHostName());

      // Register the service again with different port. This is to simulate config update
      service.register(new Discoverable("test.service",
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 4321)));

      // Should have a new discoverable
      discoverables = queue.poll(5, TimeUnit.SECONDS);
      Assert.assertNotNull(discoverables);
      Assert.assertEquals(1, discoverables.size());

      // The discoverable should have hostname the same as the k8s service name and port from the discoverable
      discoverable = discoverables.stream().findFirst()
          .orElseThrow(Exception::new);
      Assert.assertEquals(4321, discoverable.getSocketAddress().getPort());
      Assert.assertArrayEquals(new byte[0], discoverable.getPayload());
      Assert.assertEquals("cdap-test-test-service." + namespace,
          discoverable.getSocketAddress().getHostName());

      // (CDAP-19415) Add a discovery of a new service. This would force recreating a new watch.
      service.discover("test.service2").watchChanges(sd -> {
        queue.add(StreamSupport.stream(sd.spliterator(), false)
            .collect(Collectors.toSet()));
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

      discoverable = discoverables.stream().findFirst()
          .orElseThrow(Exception::new);
      Assert.assertEquals(6789, discoverable.getSocketAddress().getPort());
      Assert.assertEquals("https",
          new String(discoverable.getPayload(), StandardCharsets.UTF_8));
      Assert.assertEquals("cdap-test-test-service2." + namespace,
          discoverable.getSocketAddress().getHostName());

    } finally {
      // Cleanup the created service
      CoreV1Api api = new CoreV1Api(API_CLIENT_FACTORY.create());
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      api.deleteNamespacedService("cdap-test-test-service", namespace, null,
          null, null, null, null, deleteOptions);
      api.deleteNamespacedService("cdap-test-test-service2", namespace, null,
          null, null, null, null, deleteOptions);
    }
  }

  @Test
  @Ignore
  public void testCloseWatchRace() throws Exception {
    String namespace = "default";
    String prefix = "cdap-test-";
    try (KubeDiscoveryService service = new KubeDiscoveryService(namespace,
        prefix,
        ImmutableMap.of("cdap.container", "test"),
        Collections.emptyList(), API_CLIENT_FACTORY)) {
      // Register two services first
      service.register(new Discoverable("test1",
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
      service.register(new Discoverable("test2",
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678)));

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

      Discoverable discoverable = discoverableCompletion.get(10,
          TimeUnit.SECONDS);
      Assert.assertNotNull(discoverable);
      Assert.assertEquals(prefix + "test2." + namespace,
          discoverable.getSocketAddress().getHostName());
    } finally {
      CoreV1Api api = new CoreV1Api(API_CLIENT_FACTORY.create());
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      api.deleteNamespacedService(prefix + "test1", namespace, null, null, null,
          null, null, deleteOptions);
      api.deleteNamespacedService(prefix + "test2", namespace, null, null, null,
          null, null, deleteOptions);
    }
  }

  @Test
  @Ignore
  public void testCloseWatchRaceDifferentThreads() throws Exception {
    String namespace = "default";
    String prefix = "cdap-test-";
    try (KubeDiscoveryService service = new KubeDiscoveryService(namespace,
        prefix,
        ImmutableMap.of("cdap.container", "test"),
        Collections.emptyList(), API_CLIENT_FACTORY)) {
      // Register two services first
      service.register(new Discoverable("test1",
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 1234)));
      service.register(new Discoverable("test2",
          new InetSocketAddress(InetAddress.getLoopbackAddress(), 5678)));

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
      Assert.assertEquals(prefix + "test2." + namespace,
          discoverable.getSocketAddress().getHostName());
    } finally {
      CoreV1Api api = new CoreV1Api(API_CLIENT_FACTORY.create());
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      api.deleteNamespacedService(prefix + "test1", namespace, null, null, null,
          null, null, deleteOptions);
      api.deleteNamespacedService(prefix + "test2", namespace, null, null, null,
          null, null, deleteOptions);
    }
  }

  @Test
  public void testToDiscoverableClusterIp() {
    V1Service service = getClusterIpService();
    Discoverable expected = new Discoverable(CLUSTER_IP_SERVICE_NAME,
        InetSocketAddress.createUnresolved(
            NAME_PREFIX + CLUSTER_IP_SERVICE_NAME + ".default", 80),
        TEST_PAYLOAD.getBytes(StandardCharsets.UTF_8));

    Set<Discoverable> discoverables =
        kubeDiscoveryService.toDiscoverables(CLUSTER_IP_SERVICE_NAME, service,
            "default");

    Assert.assertEquals(expected, discoverables.iterator().next());
  }

  @Test
  public void testToDiscoverableLoadBalancer() {
    V1Service service = getLoadBalancerService();
    Discoverable expected = new Discoverable(LOAD_BALANCER_SERVICE_NAME,
        InetSocketAddress.createUnresolved(LOAD_BALANCER_IP, SERVICE_PORT),
        TEST_PAYLOAD.getBytes(StandardCharsets.UTF_8));

    Set<Discoverable> discoverables =
        kubeDiscoveryService.toDiscoverables(LOAD_BALANCER_SERVICE_NAME,
            service,
            "default");

    Assert.assertEquals(expected, discoverables.iterator().next());
  }

  @Test
  public void testToDiscoverableLoadBalancerNoIngress() {
    V1Service service = getLoadBalancerService();
    service.setStatus(new V1ServiceStatus());

    Set<Discoverable> discoverables =
        kubeDiscoveryService.toDiscoverables("my-service", service, "default");

    Assert.assertTrue(discoverables.isEmpty());
  }

  @Test
  public void testCreateClusterIpService() {
    Discoverable discoverable = new Discoverable(CLUSTER_IP_SERVICE_NAME,
        InetSocketAddress.createUnresolved("cdap.io", SERVICE_PORT),
        TEST_PAYLOAD.getBytes(StandardCharsets.UTF_8));
    V1Service expected = getClusterIpService();

    Assert.assertEquals(expected,
        kubeDiscoveryService.createService(
            NAME_PREFIX + CLUSTER_IP_SERVICE_NAME,
            discoverable));
  }

  @Test
  public void testCreateLoadBalancerService() {
    Discoverable discoverable = new Discoverable(LOAD_BALANCER_SERVICE_NAME,
        InetSocketAddress.createUnresolved(LOAD_BALANCER_IP, SERVICE_PORT),
        TEST_PAYLOAD.getBytes(StandardCharsets.UTF_8));
    V1Service expected = getLoadBalancerService();
    // Status is set by k8s API server, so remove it.
    expected.setStatus(null);

    Assert.assertEquals(expected,
        kubeDiscoveryService.createService(
            NAME_PREFIX + LOAD_BALANCER_SERVICE_NAME,
            discoverable));
  }

  @Test
  public void updateClusterIpToLoadBalancer() {
    Discoverable discoverable = new Discoverable(LOAD_BALANCER_SERVICE_NAME,
        InetSocketAddress.createUnresolved(LOAD_BALANCER_IP, SERVICE_PORT),
        TEST_PAYLOAD.getBytes(StandardCharsets.UTF_8));
    V1Service currentService = getClusterIpService();
    // Change the Service Name.
    currentService.getMetadata().putLabelsItem("cdap.service",
        NAME_PREFIX + LOAD_BALANCER_SERVICE_NAME);
    currentService.getMetadata()
        .setName(NAME_PREFIX + LOAD_BALANCER_SERVICE_NAME);
    V1Service expected = getLoadBalancerService();
    expected.setStatus(null);

    Assert.assertEquals(
        expected,
        kubeDiscoveryService.updateService(currentService, discoverable));
  }

  @Test
  public void updateLoadBalancerToClusterIp() {
    Discoverable discoverable = new Discoverable(CLUSTER_IP_SERVICE_NAME,
        InetSocketAddress.createUnresolved("cdap.io", SERVICE_PORT));
    V1Service currentService = getLoadBalancerService();
    // Change the Service Name.
    currentService.getMetadata()
        .putLabelsItem("cdap.service", NAME_PREFIX + CLUSTER_IP_SERVICE_NAME);
    currentService.getMetadata().setName(NAME_PREFIX + CLUSTER_IP_SERVICE_NAME);
    // Remove the status.
    currentService.setStatus(null);
    V1Service expected = getClusterIpService();
    // Remove the payload annotation.
    expected.getMetadata().getAnnotations().remove("cdap.service.payload");

    Assert.assertEquals(
        expected,
        kubeDiscoveryService.updateService(currentService, discoverable));
  }


  @Test
  public void testIsServiceUpdateNeededClusterIpNoChange() {
    V1Service currentService = getClusterIpService();
    Discoverable discoverable = new Discoverable(CLUSTER_IP_SERVICE_NAME,
        InetSocketAddress.createUnresolved("cdap.io", SERVICE_PORT));

    Assert.assertFalse(kubeDiscoveryService.isServiceUpdateNeeded(
        currentService.getSpec().getPorts()
            .get(0), currentService, discoverable));
  }

  @Test
  public void testIsServiceUpdateNeededClusterIpChange() {
    V1Service currentService = getClusterIpService();
    Discoverable discoverable = new Discoverable(CLUSTER_IP_SERVICE_NAME,
        InetSocketAddress.createUnresolved("cdap.io", SERVICE_PORT + 1));

    Assert.assertTrue(kubeDiscoveryService.isServiceUpdateNeeded(
        currentService.getSpec().getPorts()
            .get(0), currentService, discoverable));
  }

  @Test
  public void testIsServiceUpdateNeededLoadBalancerNoChange() {
    V1Service currentService = getLoadBalancerService();
    Discoverable discoverable = new Discoverable(LOAD_BALANCER_SERVICE_NAME,
        InetSocketAddress.createUnresolved(LOAD_BALANCER_IP, SERVICE_PORT));

    Assert.assertFalse(kubeDiscoveryService.isServiceUpdateNeeded(
        currentService.getSpec().getPorts()
            .get(0), currentService, discoverable));
  }

  @Test
  public void testIsServiceUpdateNeededLoadBalancerIpChange() {
    V1Service currentService = getLoadBalancerService();
    Discoverable discoverable = new Discoverable(LOAD_BALANCER_SERVICE_NAME,
        InetSocketAddress.createUnresolved(LOAD_BALANCER_IP + "1",
            SERVICE_PORT));

    Assert.assertTrue(kubeDiscoveryService.isServiceUpdateNeeded(
        currentService.getSpec().getPorts()
            .get(0), currentService, discoverable));
  }

  @Test
  public void testIsServiceUpdateNeededServiceTypeChange() {
   V1Service currentService = getLoadBalancerService();
   Discoverable discoverable = new Discoverable(LOAD_BALANCER_SERVICE_NAME,
        InetSocketAddress.createUnresolved("cdap.io", SERVICE_PORT));

    Assert.assertTrue(kubeDiscoveryService.isServiceUpdateNeeded(
        currentService.getSpec().getPorts()
            .get(0), currentService, discoverable));
  }

  private V1Service getLoadBalancerService() {
    HashMap<String, String> annotations = new HashMap<>(
        LOAD_BALANCER_ANNOTATIONS);
    annotations.put("cdap.service.payload", ENCODED_PAYLOAD);
    V1ObjectMeta metadata = new V1ObjectMeta()
        .name(NAME_PREFIX + LOAD_BALANCER_SERVICE_NAME)
        .ownerReferences(Collections.singletonList(OWNER_REFERENCE))
        .annotations(annotations)
        .labels(
            Collections.singletonMap("cdap.service",
                NAME_PREFIX + LOAD_BALANCER_SERVICE_NAME));

    List<V1ServicePort> ports = Collections.singletonList(
        new V1ServicePortBuilder().withPort(80).build()
    );

    V1ServiceSpec spec = new V1ServiceSpecBuilder()
        .withType("LoadBalancer")
        .withPorts(ports)
        .withSelector(POD_LABELS)
        .build();

    V1ServiceStatus status = new V1ServiceStatusBuilder()
        .withLoadBalancer(
            new V1LoadBalancerStatus()
                .addIngressItem(
                    new V1LoadBalancerIngressBuilder()
                        .withIp(LOAD_BALANCER_IP)
                        .build()
                )
        ).build();

    return new V1ServiceBuilder()
        .withMetadata(metadata)
        .withSpec(spec)
        .withStatus(status)
        .build();
  }

  private V1Service getClusterIpService() {
    V1ObjectMeta metadata = new V1ObjectMeta()
        .name(NAME_PREFIX + CLUSTER_IP_SERVICE_NAME)
        .annotations(Collections.singletonMap("cdap.service.payload",
            ENCODED_PAYLOAD))
        .ownerReferences(Collections.singletonList(OWNER_REFERENCE))
        .annotations(Collections.singletonMap("cdap.service.payload",
            ENCODED_PAYLOAD))
        .labels(
            Collections.singletonMap("cdap.service",
                NAME_PREFIX + CLUSTER_IP_SERVICE_NAME));

    List<V1ServicePort> ports = Collections.singletonList(
        new V1ServicePortBuilder().withPort(SERVICE_PORT).build()
    );

    V1ServiceSpec spec = new V1ServiceSpecBuilder()
        .withPorts(ports)
        .withType("ClusterIP")
        .withSelector(POD_LABELS)
        .build();

    return new V1ServiceBuilder()
        .withMetadata(metadata)
        .withSpec(spec)
        .build();
  }
}
