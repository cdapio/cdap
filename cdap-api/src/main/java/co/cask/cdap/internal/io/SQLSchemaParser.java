/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.internal.io;

import co.cask.cdap.api.data.schema.Schema;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
 * Parses a SQL like schema into a {@link Schema}.
 */
public final class SQLSchemaParser {
  private String schema;
  private int pos;
  private int end;
  // used so record names are unique
  private int recordNum;

  public SQLSchemaParser(String schema) {
    // replace all whitespace with a single space
    this.schema = schema.trim().replaceAll("\\s+", " ").toLowerCase();
    this.pos = 0;
    this.end = this.schema.length();
    this.recordNum = 1;
  }

  // name type, name type, ...
  public Schema parse() throws IOException {
    try {
      List<Schema.Field> fields = Lists.newArrayList();

      while (pos < end) {
        int nameEnd = schema.indexOf(" ", pos);
        errorIf(nameEnd < 0, "Expecting whitespace between column name and type");
        String name = schema.substring(pos, nameEnd);
        pos = nameEnd + 1;
        errorIf(pos >= end, "Unexpected EOF");
        fields.add(Schema.Field.of(name, parseType()));
        // stop if we're at the last field
        if (pos >= end) {
          break;
        }
        advancePastComma();
      }

      return Schema.recordOf("rec", fields);
    } catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      // can happen if, for example, there are multiple fields in a record with the same name
      throw new IOException(e);
    }
  }

  private Schema parseType() throws IOException {
    // null, boolean, int, long, float, double, bytes, or string
    Schema type;
    if (schema.startsWith("boolean", pos)) {
      pos += 7;
      type = Schema.of(Schema.Type.BOOLEAN);
    } else if (schema.startsWith("int", pos)) {
      pos += 3;
      type = Schema.of(Schema.Type.INT);
    } else if (schema.startsWith("long", pos)) {
      pos += 4;
      type = Schema.of(Schema.Type.LONG);
    } else if (schema.startsWith("float", pos)) {
      pos += 5;
      type = Schema.of(Schema.Type.FLOAT);
    } else if (schema.startsWith("double", pos)) {
      pos += 6;
      type = Schema.of(Schema.Type.DOUBLE);
    } else if (schema.startsWith("bytes", pos)) {
      pos += 5;
      type = Schema.of(Schema.Type.BYTES);
    } else if (schema.startsWith("string", pos)) {
      pos += 6;
      type = Schema.of(Schema.Type.STRING);
    } else if (schema.startsWith("null", pos)) {
      pos += 4;
      type = Schema.of(Schema.Type.NULL);
    } else if (schema.startsWith("array<", pos)) {
      type = parseArray();
    } else if (schema.startsWith("map<", pos)) {
      type = parseMap();
    } else if (schema.startsWith("record<", pos)) {
      type = parseRecord();
    } else if (schema.startsWith("union<", pos)) {
      type = parseUnion();
    } else {
      throw new IOException("Unknown data type");
    }
    if (schema.startsWith(" not null", pos)) {
      pos += 9;
      return type;
    } else {
      return Schema.nullableOf(type);
    }
  }

  // record<col1:type1,col2:type2,...>
  private Schema parseRecord() throws IOException {
    // move past the 'record<'
    advanceBy(7);
    advancePastOptionalSpace();
    String recordName = "rec" + recordNum;
    recordNum++;
    List<Schema.Field> fields = Lists.newArrayList();

    // keep going until we get to the enclosing '>'
    while (true) {
      // colName:type
      int colonPos = schema.indexOf(":", pos);
      errorIf(colonPos < 0, "Expecting a ':' between field name and type");
      String colName = schema.substring(pos, colonPos);
      pos = colonPos + 1;
      errorIf(pos >= end, "Unexpected EOF");
      fields.add(Schema.Field.of(colName, parseType()));
      // must be at the end or at a comma
      if (tryAdvancePastEndBracket()) {
        break;
      }
      advancePastComma();
    }

    return Schema.recordOf(recordName, fields);
  }

  // map<type,type>
  private Schema parseMap() throws IOException {
    // move past the 'map<'
    advanceBy(4);
    advancePastOptionalSpace();
    Schema keyType = parseType();
    // key and value must be separated by a comma
    advancePastComma();
    Schema valueType = parseType();
    // must end with '>' or ' >'
    errorIf(!tryAdvancePastEndBracket(), "Expecting an end bracket '>' to enclose the map");
    return Schema.mapOf(keyType, valueType);
  }

  // union<type,...>
  private Schema parseUnion() throws IOException {
    // move past the 'union<'
    advanceBy(6);
    advancePastOptionalSpace();
    List<Schema> unionTypes = Lists.newArrayList();

    // keep going until we see the closing '>'
    while (true) {
      unionTypes.add(parseType());
      if (tryAdvancePastEndBracket()) {
        break;
      }
      advancePastComma();
    }

    return Schema.unionOf(unionTypes);
  }

  // array<type>
  private Schema parseArray() throws IOException {
    // move past the 'array<'
    advanceBy(6);
    advancePastOptionalSpace();
    Schema componentType = parseType();
    errorIf(!tryAdvancePastEndBracket(), "Expecting an end bracket '>' to enclose the array");
    return Schema.arrayOf(componentType);
  }

  private boolean tryAdvancePastEndBracket() {
    advancePastOptionalSpace();
    if (schema.charAt(pos) == '>') {
      pos++;
      return true;
    }
    return false;
  }

  private void advancePastOptionalSpace() {
    if (schema.charAt(pos) == ' ') {
      pos++;
    }
  }

  // move past a comma and optional whitespace.
  // in other words, moves past a ", " or ",".
  private void advancePastComma() throws IOException {
    advancePastOptionalSpace();
    errorIfCharNot(',');
    advanceBy(1);
    advancePastOptionalSpace();
  }

  // advance x characters in the schema and check that we're not at the end
  private void advanceBy(int x) throws IOException {
    pos += x;
    errorIf(pos >= end, "Not expecting EOF");
  }

  private void errorIfCharNot(char c) throws IOException {
    errorIf(schema.charAt(pos) != c, "Expected a '" + c + "'");
  }

  private void errorIf(boolean condition, String errMsg) throws IOException {
    if (condition) {
      throw new IOException(String.format("schema is malformed. %s.", errMsg));
    }
  }
}
