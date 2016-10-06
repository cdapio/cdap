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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.service.ServiceDiscoverable;
import co.cask.cdap.gateway.discovery.VersionFilteredServiceDiscovered;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ProgramId;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Tests for {@link VersionFilteredServiceDiscovered}
 */
public class VersionFilteredServiceDiscoveredTest {

  @Test
  public void testVersion() throws Exception {
    ProgramId serviceId = new ApplicationId("n1", "a1").service("s1");
    String discoverableName = ServiceDiscoverable.getName(serviceId);
    List<Discoverable> candidates = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      candidates.add(new Discoverable(discoverableName, null, Bytes.toBytes(Integer.toString(i))));
      candidates.add(new Discoverable(discoverableName, null, Bytes.toBytes(Integer.toString(i))));
    }
    SimpleServiceDiscovered serviceDiscovered = new SimpleServiceDiscovered(candidates);

    // With '1' as the filter, only that version should be returned
    VersionFilteredServiceDiscovered filteredServiceDiscovered = new VersionFilteredServiceDiscovered(
      serviceDiscovered, "1");
    Iterator<Discoverable> filteredIterator = filteredServiceDiscovered.iterator();
    while (filteredIterator.hasNext()) {
      Assert.assertArrayEquals("1".getBytes(), filteredIterator.next().getPayload());
    }

    // With null, all the versions should be returned
    filteredServiceDiscovered = new VersionFilteredServiceDiscovered(serviceDiscovered, null);
    filteredIterator = filteredServiceDiscovered.iterator();
    Set<String> versions = new HashSet<>();
    while (filteredIterator.hasNext()) {
      versions.add(Bytes.toString(filteredIterator.next().getPayload()));
    }
    Assert.assertTrue(versions.size() == 5);
  }

  private static class SimpleServiceDiscovered implements ServiceDiscovered {
    private final List<Discoverable> discoverables;

    SimpleServiceDiscovered(List<Discoverable> discoverables) {
      this.discoverables = discoverables;
    }

    @Override
    public String getName() {
      return null;
    }

    @Override
    public Cancellable watchChanges(ChangeListener changeListener, Executor executor) {
      return null;
    }

    @Override
    public boolean contains(Discoverable discoverable) {
      return false;
    }

    @Override
    public Iterator<Discoverable> iterator() {
      return discoverables.iterator();
    }
  }
}
