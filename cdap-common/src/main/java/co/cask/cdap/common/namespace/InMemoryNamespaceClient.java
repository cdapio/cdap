/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.common.namespace;

import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthenticatedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Singleton;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory implementation of {@link AbstractNamespaceClient} to be used in tests. This is used in tests where
 * AppFabricServer (which runs the namespaces REST APIs) is not started, and an im-memory implementation of
 * {@link NamespaceAdmin} is not available either. Only handles namespaces right now, does not handle datasets and apps,
 * so when you delete a namespace using this client, only the namespace will be deleted, the apps and datasets will
 * stay, and will have to be deleted separately.
 */
@Singleton
public class InMemoryNamespaceClient extends AbstractNamespaceClient {

  private final List<NamespaceMeta> namespaces = new ArrayList<>();

  @Override
  public List<NamespaceMeta> list() throws Exception {
    return namespaces;
  }

  @Override
  public NamespaceMeta get(final Id.Namespace namespaceId) throws Exception {
    Iterable<NamespaceMeta> filtered = Iterables.filter(namespaces, new Predicate<NamespaceMeta>() {
      @Override
      public boolean apply(NamespaceMeta input) {
        return input.getName().equals(namespaceId.getId());
      }
    });
    if (Iterables.size(filtered) == 0) {
      throw new NamespaceNotFoundException(namespaceId);
    }
    return filtered.iterator().next();
  }

  @Override
  public boolean exists(Id.Namespace namespaceId) throws Exception {
    try {
      get(namespaceId);
    } catch (NotFoundException e) {
      return false;
    }
    return true;
  }

  @Override
  public void create(NamespaceMeta metadata) throws Exception {
    namespaces.add(metadata);
  }

  @Override
  public void delete(final Id.Namespace namespaceId) throws Exception {
    Iterables.removeIf(namespaces, new Predicate<NamespaceMeta>() {
      @Override
      public boolean apply(NamespaceMeta input) {
        return input.getName().equals(namespaceId.getId());
      }
    });
  }

  @Override
  public void deleteDatasets(Id.Namespace namespaceId) throws Exception {
    // No-op, we're not managing apps and datasets within InMemoryNamespaceAdmin yet
  }

  @Override
  public void updateProperties(Id.Namespace namespaceId, NamespaceMeta namespaceMeta) throws Exception {
    delete(namespaceId);
    create(namespaceMeta);
  }

  @Override
  protected HttpResponse execute(HttpRequest request) throws IOException, UnauthenticatedException {
    // not needed since we do not use HTTP here
    return null;
  }

  @Override
  protected URL resolve(String resource) throws IOException {
    // not needed since we do not use HTTP here
    return null;
  }
}
