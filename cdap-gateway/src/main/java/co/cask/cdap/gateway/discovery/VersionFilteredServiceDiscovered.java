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
package co.cask.cdap.gateway.discovery;

import co.cask.cdap.api.common.Bytes;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.twill.common.Cancellable;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.ServiceDiscovered;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.Executor;
import javax.annotation.Nullable;

/**
 * Return {@link Discoverable}s that match the version provided if one is provided.
 */
public class VersionFilteredServiceDiscovered implements ServiceDiscovered {

  private final ServiceDiscovered delegate;
  private final byte[] version;
  private final Predicate<Discoverable> predicate = new Predicate<Discoverable>() {
    @Override
    public boolean apply(Discoverable input) {
      return Arrays.equals(version, input.getPayload());
    }
  };

  public VersionFilteredServiceDiscovered(ServiceDiscovered delegate, @Nullable String version) {
    this.delegate = delegate;
    this.version = version == null ? null : Bytes.toBytes(version);
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Cancellable watchChanges(ChangeListener changeListener, Executor executor) {
    return delegate.watchChanges(changeListener, executor);
  }

  @Override
  public boolean contains(Discoverable discoverable) {
    return delegate.contains(discoverable);
  }

  @Override
  public Iterator<Discoverable> iterator() {
    if (version == null) {
      return delegate.iterator();
    }
    return Iterators.filter(delegate.iterator(), predicate);
  }
}
