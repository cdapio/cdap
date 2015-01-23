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
    this.schema = schema.trim().toLowerCase();
    this.pos = 0;
    this.end = this.schema.length();
    this.recordNum = 1;
  }

  // name type, name type, ...
  public Schema parse() throws IOException {
    try {
      List<Schema.Field> fields = Lists.newArrayList();

      while (pos < end) {
        String name = nextToken();
        expectWhitespace("Expecting whitespace between column name and type");
        skipWhitespace();
        errorIf(pos >= end, "Unexpected EOF");
        fields.add(Schema.Field.of(name, parseType()));
        // stop if we're at the last field
        if (pos >= end) {
          break;
        }
        advancePastComma("Expected a comma separating schema columns");
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
    String typeStr = nextToken();
    if ("boolean".equals(typeStr)) {
      type = Schema.of(Schema.Type.BOOLEAN);
    } else if ("int".equals(typeStr)) {
      type = Schema.of(Schema.Type.INT);
    } else if ("long".equals(typeStr)) {
      type = Schema.of(Schema.Type.LONG);
    } else if ("float".equals(typeStr)) {
      type = Schema.of(Schema.Type.FLOAT);
    } else if ("double".equals(typeStr)) {
      type = Schema.of(Schema.Type.DOUBLE);
    } else if ("bytes".equals(typeStr)) {
      type = Schema.of(Schema.Type.BYTES);
    } else if ("string".equals(typeStr)) {
      type = Schema.of(Schema.Type.STRING);
    } else if ("null".equals(typeStr)) {
      type = Schema.of(Schema.Type.NULL);
    } else if ("array".equals(typeStr)) {
      type = parseArray();
    } else if ("map".equals(typeStr)) {
      type = parseMap();
    } else if ("record".equals(typeStr)) {
      type = parseRecord();
    } else if ("union".equals(typeStr)) {
      type = parseUnion();
    } else {
      throw new IOException("Unknown data type " + typeStr);
    }

    skipWhitespace();
    if (schema.startsWith("not null", pos)) {
      pos += 8;
      return type;
    } else {
      return Schema.nullableOf(type);
    }
  }

  // <col1:type1,col2:type2,...>
  private Schema parseRecord() throws IOException {
    expectChar('<', "record must be followed with a '<'");
    skipWhitespace();
    String recordName = "rec" + recordNum;
    recordNum++;
    List<Schema.Field> fields = Lists.newArrayList();

    // keep going until we get to the enclosing '>'
    while (true) {
      // colName:type
      String colName = nextToken();
      errorIf(schema.charAt(pos) != ':', "Expecting a ':' between field name and type");
      pos++;
      errorIf(pos >= end, "Unexpected EOF");
      fields.add(Schema.Field.of(colName, parseType()));
      // must be at the end or at a comma
      if (tryAdvancePastEndBracket()) {
        break;
      }
      advancePastComma("Expected a comma separating record fields");
    }

    return Schema.recordOf(recordName, fields);
  }

  // <type,type>
  private Schema parseMap() throws IOException {
    expectChar('<', "map must be followed by a '<'");
    skipWhitespace();
    Schema keyType = parseType();
    // key and value must be separated by a comma
    advancePastComma("Expected a comma separating map key and value types");
    Schema valueType = parseType();
    skipWhitespace();
    expectChar('>', "map must end with a '>'");
    return Schema.mapOf(keyType, valueType);
  }

  // <type,...>
  private Schema parseUnion() throws IOException {
    expectChar('<', "union must be followed by a '<'");
    skipWhitespace();
    List<Schema> unionTypes = Lists.newArrayList();

    // keep going until we see the closing '>'
    while (true) {
      unionTypes.add(parseType());
      if (tryAdvancePastEndBracket()) {
        break;
      }
      advancePastComma("Expected a comma separating union types");
    }

    return Schema.unionOf(unionTypes);
  }

  // <type>
  private Schema parseArray() throws IOException {
    expectChar('<', "array must be followed by a '<'");
    skipWhitespace();
    Schema componentType = parseType();
    skipWhitespace();
    expectChar('>', "array must end with a '>'");
    return Schema.arrayOf(componentType);
  }

  private boolean tryAdvancePastEndBracket() {
    skipWhitespace();
    if (schema.charAt(pos) == '>') {
      pos++;
      return true;
    }
    return false;
  }

  private void skipWhitespace() {
    while (pos < end && Character.isWhitespace(schema.charAt(pos))) {
      pos++;
    }
  }

  // advances forward past the next token and returns the token.
  // in other words, goes forward until it sees whitespace or ':' or ',' or '<' or '>'.
  private String nextToken() {
    char currChar = schema.charAt(pos);
    int endPos = pos;
    while (!(endPos == end || Character.isWhitespace(currChar) ||
      currChar == ':' || currChar == ',' || currChar == '<' || currChar == '>')) {
      endPos++;
      currChar = schema.charAt(endPos);
    }
    String token = schema.substring(pos, endPos);
    pos = endPos;
    return token;
  }

  // move past a comma and optional whitespace.
  // in other words, moves past a ", " or ",".
  private void advancePastComma(String errMsg) throws IOException {
    skipWhitespace();
    expectChar(',', errMsg);
    skipWhitespace();
  }

  // error if the current character is not what is expected, and move past the character
  private void expectChar(char c, String errMsg) throws IOException {
    errorIf(schema.charAt(pos) != c, errMsg);
    pos++;
  }

  // error if the current character is not whitespace, and move past the whitespace
  private void expectWhitespace(String errMsg) throws IOException {
    errorIf(!Character.isWhitespace(schema.charAt(pos)), errMsg);
    skipWhitespace();
  }

  private void errorIf(boolean condition, String errMsg) throws IOException {
    if (condition) {
      throw new IOException(String.format("schema is malformed. %s.", errMsg));
    }
  }
}
