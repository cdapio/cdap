package com.continuuity.discovery;

import com.continuuity.app.guice.BigMamaModule;
import com.continuuity.base.Cancellable;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data.runtime.DataFabricModules;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.junit.Assert;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 *
 */
public class DiscoveryServiceTest {

  @Test
  public void testDiscovery() {
    final CConfiguration configuration = CConfiguration.create();
    configuration.set("app.temp.dir", "/tmp/app/temp");
    configuration.set("app.output.dir", "/tmp/app/archive" + UUID.randomUUID());

    Injector injector = Guice.createInjector(new DataFabricModules().getInMemoryModules(),
                                             new BigMamaModule(configuration));

    DiscoveryService service = injector.getInstance(DiscoveryService.class);
    DiscoveryServiceClient client = injector.getInstance(DiscoveryServiceClient.class);

    service.startAndWait();
    client.startAndWait();

    Iterable<Discoverable> discoverables = client.discover("test");
    Assert.assertTrue(Iterables.isEmpty(discoverables));

    List<Cancellable> cancels = Lists.newArrayList();
    cancels.add(service.register(new DiscoverableImpl("test", new InetSocketAddress(45678))));
    cancels.add(service.register(new DiscoverableImpl("test", new InetSocketAddress(12345))));

    Set<Integer> ports = Sets.newHashSet(
    Iterables.transform(discoverables, new Function<Discoverable, Integer>() {
      @Override
      public Integer apply(Discoverable input) {
        return input.getSocketAddress().getPort();
      }
    }));

    Assert.assertEquals(ImmutableSet.of(12345,45678), ports);

    for (Cancellable cancellable : cancels) {
      cancellable.cancel();
    }

    Assert.assertTrue(Iterables.isEmpty(discoverables));

    service.stopAndWait();
    client.stopAndWait();
  }

  private static final class DiscoverableImpl implements Discoverable {

    private final String name;
    private final InetSocketAddress address;

    private DiscoverableImpl(String name, InetSocketAddress address) {
      this.name = name;
      this.address = address;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
      return address;
    }
  }
}
