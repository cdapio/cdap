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

package io.cdap.cdap.graphql.store.namespace.datafetchers;

import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.objects.Artifact;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.NamespaceId;

/**
 * Fetchers to get applications
 */
public class NamespaceDataFetcher {

  /**
   * Fetcher to get namespaces
   *
   * @return the data fetcher
   */
  public DataFetcher getNamespacesDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        // namespaceAdmin.list().stream()
        // .map(meta -> new NamespaceMeta.Builder(meta).buildWithoutKeytabURIVersion())
        // .collect(Collectors.toList())
        throw new UnsupportedOperationException("Implement");
      }
    );
  }

  // TODO naming of method a bit weird? Talking about implementation?

  /**
   * Fetcher to get a namespace. The request originates from other fetchers that query a namespace
   *
   * @return the data fetcher
   */
  public DataFetcher getNamespaceFromSourceDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        // Artifact artifact = dataFetchingEnvironment.getSource();
        // String namespace = artifact.getNamespace();
        //
        // return getNamespace(namespace);
        throw new UnsupportedOperationException("Implement");
      }
    );
  }

  // TODO naming of method a bit weird? Talking about implementation?

  /**
   * Fetcher to get a namespace. The request originates from a query
   *
   * @return the data fetcher
   */
  public DataFetcher getNamespaceFromQueryDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        // String name = dataFetchingEnvironment.getArgument(GraphQLFields.NAME);
        //
        // return getNamespace(name);
        throw new UnsupportedOperationException("Implement");
      }
    );
  }

  private NamespaceMeta getNamespace(String namespace) throws Exception {
    // return new NamespaceMeta.Builder(
    //   namespaceAdmin.get(new NamespaceId(namespace))
    // ).buildWithoutKeytabURIVersion();
    throw new UnsupportedOperationException("Implement");
  }
}
