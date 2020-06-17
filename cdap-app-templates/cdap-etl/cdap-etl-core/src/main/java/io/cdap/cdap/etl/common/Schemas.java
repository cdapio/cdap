/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.etl.common;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.data.schema.SchemaHash;

/**
 * Schema utility methods
 */
public class Schemas {

  private Schemas() {
    // no-op constructor for utility class
  }

  public static boolean equalsIgnoringRecordName(Schema s1, Schema s2) {
    SchemaHash hash1 = new SchemaHash(s1, false);
    SchemaHash hash2 = new SchemaHash(s2, false);
    return hash1.equals(hash2);
  }
}
