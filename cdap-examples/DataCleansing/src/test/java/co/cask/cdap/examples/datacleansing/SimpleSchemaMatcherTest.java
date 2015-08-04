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

package co.cask.cdap.examples.datacleansing;

import co.cask.cdap.api.data.schema.Schema;
import com.google.gson.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class SimpleSchemaMatcherTest {
    Schema simplePersonSchema =
      Schema.recordOf("person",
                      Schema.Field.of("pid", Schema.of(Schema.Type.LONG)),
                      Schema.Field.of("name", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("dob", Schema.of(Schema.Type.STRING)),
                      Schema.Field.of("zip", Schema.nullableOf(Schema.of(Schema.Type.INT)))
      );

  @Test
  public void testMatches() {
    SimpleSchemaMatcher schemaMatcher = new SimpleSchemaMatcher(simplePersonSchema);

    // all fields present
    JsonObject jsonObject = new JsonObject();
    jsonObject.addProperty("pid", 223986723L);
    jsonObject.addProperty("name", "bob");
    jsonObject.addProperty("dob", "5-20-92");
    jsonObject.addProperty("zip", "84125");

    Assert.assertTrue((schemaMatcher.matches(jsonObject.toString())));

    // zip field is nullable, so allowed to be missing
    jsonObject = new JsonObject();
    jsonObject.addProperty("pid", 223986723L);
    jsonObject.addProperty("name", "bob");
    jsonObject.addProperty("dob", "5-20-92");

    Assert.assertTrue((schemaMatcher.matches(jsonObject.toString())));

    // pid is required, so matcher will not match
    jsonObject = new JsonObject();
    jsonObject.addProperty("name", "bob");
    jsonObject.addProperty("dob", "5-20-92");
    jsonObject.addProperty("zip", "95217");

    Assert.assertFalse((schemaMatcher.matches(jsonObject.toString())));

    // zip is type INT, so having characters in it will not work
    jsonObject = new JsonObject();
    jsonObject.addProperty("pid", 223986723L);
    jsonObject.addProperty("name", "bob");
    jsonObject.addProperty("dob", "5-20-92");
    jsonObject.addProperty("zip", "84125qq");

    Assert.assertFalse((schemaMatcher.matches(jsonObject.toString())));
  }

  @Test
  public void testNonSimpleSchema() {
    Schema complexSchema = Schema.recordOf("complexSchema", Schema.Field.of("person", simplePersonSchema));

    JsonObject simplePersonJson = new JsonObject();
    simplePersonJson.addProperty("pid", 223986723L);
    simplePersonJson.addProperty("name", "bob");
    simplePersonJson.addProperty("dob", "5-20-92");
    simplePersonJson.addProperty("zip", "84125");

    JsonObject personWrapperJson = new JsonObject();
    personWrapperJson.add("person", simplePersonJson);
    // this fails because it uses non-simple types as the fields of the main schema
    Assert.assertFalse(new SimpleSchemaMatcher(complexSchema).matches(personWrapperJson.toString()));
  }
}
