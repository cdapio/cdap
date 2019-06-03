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

package io.cdap.cdap.graphql.store.artifact.datafetchers;

import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.graphql.objects.Artifact;

/**
 * Fetchers to get applications
 */
public class ApplicationDataFetcher {

  /**
   * Fetcher to get applications
   *
   * @return the data fetcher
   */
  public DataFetcher getApplicationsDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        Artifact artifact = dataFetchingEnvironment.getSource();

        return artifact.getMeta().getClasses().getApps();
      }
    );
  }

}
