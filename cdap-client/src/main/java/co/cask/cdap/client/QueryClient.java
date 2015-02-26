/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.client;

import co.cask.cdap.client.config.ClientConfig;
import co.cask.cdap.explore.client.ExploreClient;
import co.cask.cdap.explore.client.ExploreExecutionResult;
import co.cask.cdap.explore.client.SuppliedAddressExploreClient;
import co.cask.cdap.proto.Id;
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableFuture;

import javax.inject.Inject;

/**
 * Provides ways to query CDAP Datasets.
 */
public class QueryClient {

  private final ClientConfig config;
  private final ExploreClient exploreClient;

  @Inject
  public QueryClient(final ClientConfig config) {
    this.config = config;

    Supplier<String> hostname = new Supplier<String>() {
      @Override
      public String get() {
        return config.getHostname();
      }
    };

    Supplier<Integer> port = new Supplier<Integer>() {
      @Override
      public Integer get() {
        return config.getPort();
      }
    };

    Supplier<String> accessToken = new Supplier<String>() {
      @Override
      public String get() {
        if (config.getAccessToken() != null) {
          return config.getAccessToken().getValue();
        }

        return null;
      }
    };

    exploreClient = new SuppliedAddressExploreClient(hostname, port, accessToken);
  }

  /**
   * Executes a query asynchronously.
   *
   * @param query query string to execute
   * @return {@link ListenableFuture} eventually containing a {@link ExploreExecutionResult} object with the results
   *         of the query, when it is done. The {@link ListenableFuture#get()} method will throw exceptions if a
   *         network error occurs, if the query is malformed, or if the query is cancelled.
   */
  public ListenableFuture<ExploreExecutionResult> execute(String query) {
    return exploreClient.submit(Id.Namespace.from(config.getNamespace()), query);
  }
}
