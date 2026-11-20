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

package io.cdap.cdap.etl.common.jdbc;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Validates JDBC connection strings and rejects dangerous driver parameters that can trigger
 * arbitrary file read, unsafe deserialization, or remote code execution.
 */
public final class JdbcUrlSanitizer {

  private static final List<String> DENIED_JDBC_PARAMS = Collections.unmodifiableList(
      Arrays.asList(
          "allowloadlocalinfile",
          "allowurlinlocalinfile",
          "allowloadlocalinfileinpath",
          "autodeserialize",
          "queryinterceptors",
          "statementinterceptors",
          "detectcustomcollations",
          "socketfactory",
          "sslfactory",
          "sslpasswordcallback",
          "authenticationpluginclassname",
          "init=",
          "script"
      )
  );

  private JdbcUrlSanitizer() {
  }

  /**
   * Validates that the provided JDBC URL does not contain disallowed driver parameters.
   *
   * @param jdbcUrl the JDBC connection URL to validate
   * @throws IllegalArgumentException if a disallowed parameter is present
   */
  public static void validateJdbcUrl(String jdbcUrl) {
    if (jdbcUrl == null) {
      return;
    }
    String normalized = jdbcUrl.toLowerCase(Locale.ROOT);
    for (String denied : DENIED_JDBC_PARAMS) {
      if (normalized.contains(denied)) {
        throw new IllegalArgumentException(
            "Disallowed JDBC connection parameter detected: " + denied);
      }
    }
  }
}
