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

import com.google.common.collect.ImmutableMap;
import graphql.schema.AsyncDataFetcher;
import graphql.schema.DataFetcher;
import io.cdap.cdap.graphql.cdap.schema.GraphQLFields;
import io.cdap.cdap.graphql.objects.Artifact;
import org.apache.twill.filesystem.Location;

/**
 * Fetchers to get locations
 */
public class LocationDataFetcher {

  /**
   * Fetcher to get a location
   *
   * @return the data fetcher
   */
  public DataFetcher getLocationDataFetcher() {
    return AsyncDataFetcher.async(
      dataFetchingEnvironment -> {
        Artifact artifact = dataFetchingEnvironment.getSource();
        Location location = artifact.getLocation();

        return ImmutableMap.of(GraphQLFields.NAME, location.toURI().getPath());
      }
    );
  }

}
