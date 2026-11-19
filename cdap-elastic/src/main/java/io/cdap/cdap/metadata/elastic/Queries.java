/*
 * Copyright © 2026 Cask Data, Inc.
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

package io.cdap.cdap.metadata.elastic;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;

/**
 * Builds the JSON of the search queries that the metadata storage sends.
 */
final class Queries {

  private Queries() {
  }

  /** Returns a query that matches every document. */
  static JsonObject matchAll() {
    return query("match_all", new JsonObject());
  }

  /** Returns a query for documents whose field has exactly the given value. */
  static JsonObject term(String field, String value) {
    JsonObject condition = new JsonObject();
    condition.addProperty("value", value);
    return fieldQuery("term", field, condition);
  }

  /**
   * Returns a query for documents whose field has exactly the given value. The query does not
   * add to the score of a document, so it only restricts the results of another query.
   */
  static JsonObject filterTerm(String field, String value) {
    JsonObject condition = new JsonObject();
    condition.addProperty("value", value);
    condition.addProperty("boost", 0.0);
    return fieldQuery("term", field, condition);
  }

  /**
   * Returns a query for documents whose field has exactly the given value. The query does not
   * add to the score of a document, so it only restricts the results of another query.
   */
  static JsonObject filterTerm(String field, boolean value) {
    JsonObject condition = new JsonObject();
    condition.addProperty("value", value);
    condition.addProperty("boost", 0.0);
    return fieldQuery("term", field, condition);
  }

  /** Returns a query for documents whose analyzed field contains all words of the text. */
  static JsonObject matchAllWords(String field, String text) {
    JsonObject condition = new JsonObject();
    condition.addProperty("query", text);
    condition.addProperty("operator", "and");
    return fieldQuery("match", field, condition);
  }

  /** Returns a query for documents whose field has a term that matches a * and ? pattern. */
  static JsonObject wildcard(String field, String pattern) {
    JsonObject condition = new JsonObject();
    condition.addProperty("value", pattern);
    return fieldQuery("wildcard", field, condition);
  }

  /**
   * Returns a query for documents that have a nested object at the path that matches the query.
   * The score of a document is the highest score of its matching nested objects.
   */
  static JsonObject nested(String path, JsonObject query) {
    JsonObject nested = new JsonObject();
    nested.addProperty("path", path);
    nested.add("query", query);
    nested.addProperty("score_mode", "max");
    return query("nested", nested);
  }

  /** Returns a query for documents that match every one of the queries. */
  static JsonObject allOf(List<JsonObject> queries) {
    JsonObject bool = new JsonObject();
    bool.add("must", toArray(queries));
    return query("bool", bool);
  }

  /** Returns a query for documents that match at least one of the queries. */
  static JsonObject anyOf(List<JsonObject> queries) {
    JsonObject bool = new JsonObject();
    bool.add("should", toArray(queries));
    bool.addProperty("minimum_should_match", 1);
    return query("bool", bool);
  }

  private static JsonObject fieldQuery(String type, String field, JsonObject condition) {
    JsonObject fieldCondition = new JsonObject();
    fieldCondition.add(field, condition);
    return query(type, fieldCondition);
  }

  private static JsonObject query(String type, JsonObject body) {
    JsonObject query = new JsonObject();
    query.add(type, body);
    return query;
  }

  private static JsonArray toArray(List<JsonObject> queries) {
    JsonArray array = new JsonArray();
    queries.forEach(array::add);
    return array;
  }
}
