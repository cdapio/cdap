/*
 *
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

package io.cdap.cdap.store.artifact.datafetchers;

import com.google.common.base.Throwables;
import graphql.schema.DataFetcher;
import io.cdap.cdap.api.artifact.ArtifactScope;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.namespace.NamespaceAdmin;
import io.cdap.cdap.internal.app.runtime.artifact.ArtifactRepository;
import io.cdap.cdap.proto.id.Ids;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.store.artifact.schema.ArtifactFields;

import java.io.IOException;
import javax.annotation.Nullable;
import javax.inject.Inject;

public class ArtifactDataFetcher {

  private final ArtifactRepository artifactRepository;
  private final NamespaceAdmin namespaceQueryAdmin;

  @Inject
  ArtifactDataFetcher(ArtifactRepository artifactRepository, NamespaceAdmin namespaceQueryAdmin) {
    this.artifactRepository = artifactRepository;
    this.namespaceQueryAdmin = namespaceQueryAdmin;
  }

  public DataFetcher getArtifactsDataFetcher() {
    return dataFetchingEnvironment -> {
      String scope = dataFetchingEnvironment.getArgument(ArtifactFields.SCOPE);
      String namespace = dataFetchingEnvironment.getArgument(ArtifactFields.NAMESPACE);

      try {
        if (scope == null) {
          NamespaceId namespaceId = validateAndGetNamespace(namespace);
          return artifactRepository.getArtifactSummaries(namespaceId, true);
        } else {
          NamespaceId namespaceId = validateAndGetScopedNamespace(Ids.namespace(namespace), scope);
          return artifactRepository.getArtifactSummaries(namespaceId, false);
        }
      } catch (IOException ioe) {
        throw new RuntimeException("Error reading artifact metadata from the store.");
      }

    };
  }

  private NamespaceId validateAndGetNamespace(String namespaceId) throws NamespaceNotFoundException {
    return validateAndGetScopedNamespace(Ids.namespace(namespaceId), ArtifactScope.USER);
  }

  // check that the namespace exists, and check if the request is only supposed to include system artifacts,
  // and returning the system namespace if so.
  private NamespaceId validateAndGetScopedNamespace(NamespaceId namespace, ArtifactScope scope)
    throws NamespaceNotFoundException {

    try {
      namespaceQueryAdmin.get(namespace);
    } catch (NamespaceNotFoundException e) {
      throw e;
    } catch (Exception e) {
      // This can only happen when NamespaceAdmin uses HTTP to interact with namespaces.
      // Within AppFabric, NamespaceAdmin is bound to DefaultNamespaceAdmin which directly interacts with MDS.
      // Hence, this should never happen.
      throw Throwables.propagate(e);
    }

    return ArtifactScope.SYSTEM.equals(scope) ? NamespaceId.SYSTEM : namespace;
  }

  private NamespaceId validateAndGetScopedNamespace(NamespaceId namespace, @Nullable String scope)
    throws NamespaceNotFoundException, BadRequestException {
    if (scope != null) {
      return validateAndGetScopedNamespace(namespace, validateScope(scope));
    }
    return validateAndGetScopedNamespace(namespace, ArtifactScope.USER);
  }

  private ArtifactScope validateScope(String scope) throws BadRequestException {
    try {
      return ArtifactScope.valueOf(scope.toUpperCase());
    } catch (IllegalArgumentException e) {
      throw new BadRequestException("Invalid scope " + scope);
    }
  }
}
