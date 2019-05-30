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

package io.cdap.cdap.starwars.datafetchers;

import graphql.schema.DataFetcher;
import io.cdap.cdap.graphql.datafetchers.DataFetchers;
import io.cdap.cdap.graphql.schema.Fields;
import io.cdap.cdap.starwars.data.Data;

/**
 * TODO
 */
public class StarWarsDataFetchers implements DataFetchers {

  /**
   * TODO
   */
  public DataFetcher getHeroDataFetcher() {
    return dataFetchingEnvironment -> {
      throw new UnsupportedOperationException("Implement");
    };
  }

  /**
   * TODO
   */
  public DataFetcher getHumanDataFetcher() {
    return dataFetchingEnvironment -> {
      String humanId = dataFetchingEnvironment.getArgument(Fields.ID);

      return Data.HUMANS.get(humanId);
    };
  }

  /**
   * TODO
   */
  public DataFetcher getDroidDataFetcher() {
    return dataFetchingEnvironment -> {
      throw new UnsupportedOperationException("Implement");
    };
  }
}
