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

package io.cdap.cdap.starwars;

import graphql.schema.DataFetcher;

class StarWarsDataFetcher {

  static DataFetcher getHeroDataFetcher() {
    return dataFetchingEnvironment -> {
      String bookId = dataFetchingEnvironment.getArgument("id");

      throw new UnsupportedOperationException("Implement");
    };
  }

  static DataFetcher getHumanDataFetcher() {
    return dataFetchingEnvironment -> {
      String bookId = dataFetchingEnvironment.getArgument("id");

      throw new UnsupportedOperationException("Implement");
      // return BOOKS
      //   .stream()
      //   .filter(book -> book.get("id").equals(bookId))
      //   .findFirst()
      //   .orElse(null);
    };
  }

  static DataFetcher getDroidDataFetcher() {
    return dataFetchingEnvironment -> {
      String bookId = dataFetchingEnvironment.getArgument("id");

      throw new UnsupportedOperationException("Implement");
    };
  }
}
