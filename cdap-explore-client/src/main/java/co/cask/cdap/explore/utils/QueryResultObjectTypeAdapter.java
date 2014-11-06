/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.explore.utils;

import co.cask.cdap.proto.QueryResult;
import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.util.List;

/**
 * Class for serialize/deserialize a QueryResult object to/from json through {@link com.google.gson.Gson Gson}.
 */
public class QueryResultObjectTypeAdapter extends TypeAdapter<QueryResult.ResultObject> {

  private static final Gson GSON = new Gson();

  @Override
  public void write(JsonWriter writer, QueryResult.ResultObject value) throws IOException {
    writer.value(GSON.toJson(value));
  }

  @Override
  public QueryResult.ResultObject read(JsonReader in) throws IOException {
    QueryResult.ResultObject tmpObject = GSON.fromJson(in, QueryResult.ResultObject.class);
    switch (tmpObject.getType()) {
      case BYTE:
        if (tmpObject.getValue() instanceof Double) {
          return QueryResult.ResultObject.of(((Double) tmpObject.getValue()).byteValue());
        }
        break;
      case SHORT:
        if (tmpObject.getValue() instanceof Double) {
          return QueryResult.ResultObject.of(((Double) tmpObject.getValue()).shortValue());
        }
        break;
      case INT:
        if (tmpObject.getValue() instanceof Double) {
          return QueryResult.ResultObject.of(((Double) tmpObject.getValue()).intValue());
        }
        break;
      case LONG:
        if (tmpObject.getValue() instanceof Double) {
          return QueryResult.ResultObject.of(((Double) tmpObject.getValue()).longValue());
        }
        break;
      case BINARY:
        if (!(tmpObject.getValue() instanceof List)) {
          throw new IOException("Binary type should be deserialized as a list.");
        }

        List list = (List) tmpObject.getValue();
        byte[] byteArr = new byte[list.size()];
        for (int i = 0; i < list.size(); i++) {
          Object obj = list.get(i);
          if (obj instanceof Double) {
            byteArr[i] = (byte) ((Double) obj).intValue();
          } else if (obj instanceof Byte) {
            byteArr[i] = (Byte) obj;
          } else {
            throw new IOException("Value type is unsupported: " + obj.getClass());
          }
        }
        return QueryResult.ResultObject.of(byteArr);
    }
    return tmpObject;
  }
}
