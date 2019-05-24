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

package io.cdap.cdap.standalone;

import com.google.common.collect.ImmutableMap;
import graphql.schema.DataFetcher;
import io.cdap.cdap.graphql.datafetchers.DataFetchers;
import io.cdap.cdap.graphql.schema.Fields;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * TODO
 */
class BooksDataFetchers implements DataFetchers {

  // TODO extract to data
  private static final List<Map<String, String>> BOOKS = Arrays.asList(
    ImmutableMap.of("id", "book-1",
                    "name", "Harry Potter and the Philosopher's Stone",
                    "pageCount", "223",
                    "authorId", "author-1"),
    ImmutableMap.of("id", "book-2",
                    "name", "Moby Dick",
                    "pageCount", "635",
                    "authorId", "author-2"),
    ImmutableMap.of("id", "book-3",
                    "name", "Interview with the vampire",
                    "pageCount", "371",
                    "authorId", "author-3")
  );

  private static final List<Map<String, String>> AUTHORS = Arrays.asList(
    ImmutableMap.of("id", "author-1",
                    "firstName", "Joanne",
                    "lastName", "Rowling"),
    ImmutableMap.of("id", "author-2",
                    "firstName", "Herman",
                    "lastName", "Melville"),
    ImmutableMap.of("id", "author-3",
                    "firstName", "Anne",
                    "lastName", "Rice")
  );

  DataFetcher getBookByIdDataFetcher() {
    return dataFetchingEnvironment -> {
      String bookId = dataFetchingEnvironment.getArgument(Fields.ID);

      return BOOKS
        .stream()
        .filter(book -> book.get(Fields.ID).equals(bookId))
        .findFirst()
        .orElse(null);
    };
  }

  DataFetcher getAuthorDataFetcher() {
    return dataFetchingEnvironment -> {
      Map<String, String> book = dataFetchingEnvironment.getSource();
      String authorId = book.get(BooksDataFields.AUTHOR_ID);

      return AUTHORS
        .stream()
        .filter(author -> author.get(Fields.ID).equals(authorId))
        .findFirst()
        .orElse(null);
    };
  }

}
