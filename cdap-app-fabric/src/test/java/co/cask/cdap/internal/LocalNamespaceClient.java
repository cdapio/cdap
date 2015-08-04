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

package co.cask.cdap.internal;

import co.cask.cdap.common.NamespaceAlreadyExistsException;
import co.cask.cdap.common.NamespaceCannotBeCreatedException;
import co.cask.cdap.common.NamespaceCannotBeDeletedException;
import co.cask.cdap.common.NamespaceNotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.common.namespace.AbstractNamespaceClient;
import co.cask.cdap.internal.app.namespace.NamespaceAdmin;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpResponse;
import com.google.inject.Inject;

import java.io.IOException;
import java.net.URL;
import java.util.List;

/**
 * NamespaceClient which directly interacts with a {@link NamespaceAdmin}
 */
public class LocalNamespaceClient extends AbstractNamespaceClient {
  private final NamespaceAdmin namespaceAdmin;

  @Inject
  public LocalNamespaceClient(NamespaceAdmin namespaceAdmin) {
    this.namespaceAdmin = namespaceAdmin;
  }

  @Override
  public List<NamespaceMeta> list() throws IOException, UnauthorizedException {
    return namespaceAdmin.listNamespaces();
  }

  @Override
  public NamespaceMeta get(String namespaceId) throws NamespaceNotFoundException, IOException, UnauthorizedException {
    return namespaceAdmin.getNamespace(Id.Namespace.from(namespaceId));
  }

  @Override
  public void delete(String namespaceId)
    throws NamespaceNotFoundException, NamespaceCannotBeDeletedException, IOException, UnauthorizedException {
    namespaceAdmin.deleteNamespace(Id.Namespace.from(namespaceId));
  }

  @Override
  public void create(NamespaceMeta namespaceMeta)
    throws NamespaceAlreadyExistsException, NamespaceCannotBeCreatedException {
    namespaceAdmin.createNamespace(namespaceMeta);
  }

  // This class overrides all public API methods to use in-memory namespaceAdmin, and so the following two are not used.
  @Override
  protected HttpResponse execute(HttpRequest request) throws IOException, UnauthorizedException {
    return null;
  }

  @Override
  protected URL resolve(String resource) throws IOException {
    return null;
  }
}
