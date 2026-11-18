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
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import io.cdap.cdap.k8s.common.AbstractWatcherThread;
import io.cdap.cdap.master.environment.k8s.ApiClientFactory;
import io.cdap.cdap.master.spi.discovery.DefaultServiceDiscovered;
import io.kubernetes.client.openapi.models.V1Endpoints;
import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.util.generic.options.ListOptions;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.twill.discovery.Discoverable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for {@link AbstractDiscoveryWatcherThread}.
 */
public class AbstractDiscoveryWatcherThreadTest {

  private static final ApiClientFactory NO_CLUSTER_API_CLIENT_FACTORY = () -> {
    throw new IOException("No Kubernetes cluster in unit test");
  };
  private static final String NAME_PREFIX = "cdap-inst-";
  private static final String SERVICE = "app.fabric";

  private Map<String, DefaultServiceDiscovered> serviceDiscovereds;
  private DefaultServiceDiscovered discovered;
  private TestWatcher watcher;

  @Before
  public void setUp() {
    serviceDiscovereds = new ConcurrentHashMap<>();
    discovered = new DefaultServiceDiscovered(SERVICE);
    serviceDiscovereds.put(SERVICE, discovered);
    // Never started, so it makes no API call. The tests drive the listener methods directly.
    watcher = new TestWatcher(serviceDiscovereds);
  }

  @Test
  public void testAddedResourceStripsPrefixAndPublishes() {
    watcher.resourceAdded(service(NAME_PREFIX + SERVICE, "host-1"));

    Discoverable discoverable = Iterables.getOnlyElement(discovered);
    Assert.assertEquals(SERVICE, discoverable.getName());
    Assert.assertEquals("host-1", discoverable.getSocketAddress().getHostString());
  }

  @Test
  public void testModifiedResourceReplacesDiscoverables() {
    watcher.resourceAdded(service(NAME_PREFIX + SERVICE, "host-1"));
    watcher.resourceModified(service(NAME_PREFIX + SERVICE, "host-2"));

    Assert.assertEquals("host-2",
        Iterables.getOnlyElement(discovered).getSocketAddress().getHostString());
  }

  @Test
  public void testDeletedResourceClearsDiscoverables() {
    watcher.resourceAdded(service(NAME_PREFIX + SERVICE, "host-1"));
    watcher.resourceDeleted(service(NAME_PREFIX + SERVICE, "host-1"));

    Assert.assertTrue(Iterables.isEmpty(discovered));
  }

  @Test
  public void testResourcesWithoutMatchingLabelAreIgnored() {
    watcher.resourceAdded(service(NAME_PREFIX + SERVICE, "host-1"));

    // Develop's service watcher threw a NullPointerException on missing metadata or labels, and a
    // StringIndexOutOfBoundsException on a label shorter than the prefix. All are now ignored.
    V1Service noMetadata = new V1Service();
    V1Service noLabels = new V1Service().metadata(new V1ObjectMeta().name("x"));
    V1Service otherLabel = new V1Service().metadata(new V1ObjectMeta().name("x")
        .labels(Collections.singletonMap("app", "x")));
    V1Service otherPrefix = service("short", "host-9");
    V1Service undiscovered = service(NAME_PREFIX + "metrics", "host-9");
    for (V1Service service : Arrays.asList(noMetadata, noLabels, otherLabel, otherPrefix,
        undiscovered)) {
      watcher.resourceAdded(service);
      watcher.resourceModified(service);
      watcher.resourceDeleted(service);
    }

    Assert.assertEquals("host-1",
        Iterables.getOnlyElement(discovered).getSocketAddress().getHostString());
    Assert.assertEquals(Collections.singleton(SERVICE), serviceDiscovereds.keySet());
  }

  @Test
  public void testLabelSelectorCoversEveryAddedServiceOnce() {
    watcher.addService(SERVICE);
    watcher.addService("metrics");
    watcher.addService(SERVICE);

    ListOptions options = new ListOptions();
    watcher.updateListOptions(options);

    String selector = options.getLabelSelector();
    String prefix = KubeDiscoveryService.SERVICE_LABEL + " in (";
    Assert.assertTrue(selector, selector.startsWith(prefix) && selector.endsWith(")"));
    String[] values = selector.substring(prefix.length(), selector.length() - 1).split(",");
    Assert.assertEquals(2, values.length);
    Assert.assertEquals(ImmutableSet.of(NAME_PREFIX + SERVICE, NAME_PREFIX + "metrics"),
        new HashSet<>(Arrays.asList(values)));
  }

  @Test
  public void testResourceTypeResolvesThroughTheGenericBase() {
    // AbstractWatcherThread decodes events into the type it resolves from getClass() this way.
    // The extra generic layer must still resolve to the concrete model class.
    Assert.assertEquals(V1Service.class, resolveResourceType(TestWatcher.class));

    try (KubeEndpointsDiscoveryService service = new KubeEndpointsDiscoveryService("default",
        NAME_PREFIX, NO_CLUSTER_API_CLIENT_FACTORY)) {
      service.discover(SERVICE);
      Assert.assertEquals(V1Endpoints.class,
          resolveResourceType(service.getWatcherThread().getClass()));
    }
  }

  private static Object resolveResourceType(Class<?> watcherClass) {
    return TypeToken.of(watcherClass)
        .resolveType(AbstractWatcherThread.class.getTypeParameters()[0]).getType();
  }

  private static V1Service service(String serviceLabel, String host) {
    return new V1Service().metadata(new V1ObjectMeta()
        .name(host)
        .labels(Collections.singletonMap(KubeDiscoveryService.SERVICE_LABEL, serviceLabel)));
  }

  /**
   * Maps a service to one {@link Discoverable} whose host is the resource name.
   */
  private static final class TestWatcher extends AbstractDiscoveryWatcherThread<V1Service> {

    TestWatcher(Map<String, DefaultServiceDiscovered> serviceDiscovereds) {
      super("test-watcher", "default", "services", NO_CLUSTER_API_CLIENT_FACTORY, NAME_PREFIX,
          serviceDiscovereds);
    }

    @Override
    protected Set<Discoverable> toDiscoverables(String name, V1Service service) {
      return Collections.singleton(new Discoverable(name,
          InetSocketAddress.createUnresolved(service.getMetadata().getName(), 80)));
    }
  }
}
