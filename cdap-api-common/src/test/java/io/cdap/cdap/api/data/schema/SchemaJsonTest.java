/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.api.data.schema;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SchemaJsonTest {

    @Test
    public void testJsonParsingSuccessWithDuplicateRecordNames() throws IOException {
        String testSchemaJson = "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":" +
                "\"A\",\"type\":{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"B\",\"type\":{\"type\":\"" +
                "record\",\"name\":\"B\",\"fields\":[{\"name\":\"D\",\"type\":{\"type\":\"record\",\"name\":\"D\",\"" +
                "fields\":[{\"name\":\"BD1\",\"type\":\"string\"},{\"name\":\"BD2\",\"type\":\"string\"}]}}]}},{\"" +
                "name\":\"C\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"D\",\"type\":{\"" +
                "type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"CD1\",\"type\":\"string\"}," +
                "{\"name\":\"CD2\",\"type\":\"string\"}]}}]}}]}}]}";
        Schema schema = Schema.parseJson(testSchemaJson);
        Assert.assertEquals(schema.getField("A").getSchema().getField("B").getSchema().
                getField("D").getSchema().getFields().get(0).getName(), "BD1");
        Assert.assertEquals(schema.getField("A").getSchema().getField("C").getSchema().
                getField("D").getSchema().getFields().get(0).getName(), "CD1");
    }

    @Test
    public void testJsonParsingSuccessWithPreviousRecordTypeReference() throws IOException {
        String testSchemaJson = "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"A\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"B\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"D\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"BD1\",\"type\":\"string\"}," +
                "{\"name\":\"BD2\",\"type\":\"string\"}]}}]}},{\"name\":\"C\",\"type\":{\"type\":\"record\"," +
                "\"name\":\"C\",\"fields\":[{\"name\":\"D\",\"type\":\"D\"}]}}]}}]}";

        Schema schema = Schema.parseJson(testSchemaJson);
        Assert.assertEquals(schema.getField("A").getSchema().getField("B").getSchema().
                getField("D").getSchema().getFields().get(0).getName(), "BD1");
        Assert.assertEquals(schema.getField("A").getSchema().getField("C").getSchema().
                getField("D").getSchema().getFields().get(0).getName(), "BD1");
    }

    @Test
    public void testJsonParsingSuccessWithPreviousEnumTypeReference() throws IOException {
        String testSchemaJson = "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"A\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"B\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"D\"," +
                "\"type\":{\"type\":\"enum\",\"name\":\"D\"}}]}},{\"name\":\"C\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"D\",\"type\":\"D\"}]}}]}}]}";

        Schema schema = Schema.parseJson(testSchemaJson);
        Assert.assertEquals(schema.getField("A").getSchema().getField("B").getSchema().
                getField("D").getSchema().getType(), Schema.Type.ENUM);
        Assert.assertEquals(schema.getField("A").getSchema().getField("C").getSchema().
                getField("D").getSchema().getType(), Schema.Type.ENUM);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testJsonParsingFailureWithEmptyFields() throws IOException {
        String testSchemaJson = "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":[{\"name\":\"A\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"B\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"D\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"D\",\"fields\":[]}}]}}]}}]}";

        Schema schema = Schema.parseJson(testSchemaJson);
    }

    @Test
    public void testJsonParsingSuccessAndCorrectSchemaToStringWithDuplicateRecordNames() throws IOException {
        String testSchemaJson = "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":" +
                "\"A\",\"type\":{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"B\",\"type\":{\"type\":\"" +
                "record\",\"name\":\"B\",\"fields\":[{\"name\":\"D\",\"type\":{\"type\":\"record\",\"name\":\"D\",\"" +
                "fields\":[{\"name\":\"BD1\",\"type\":\"string\"},{\"name\":\"BD2\",\"type\":\"string\"}]}}]}},{\"" +
                "name\":\"C\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"D\",\"type\":{\"" +
                "type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"CD1\",\"type\":\"string\"}," +
                "{\"name\":\"CD2\",\"type\":\"string\"}]}}]}}]}}]}";
        Schema schema = Schema.parseJson(testSchemaJson);
        Assert.assertEquals(schema.toString(), testSchemaJson);
    }

    @Test
    public void testJsonParsingSuccessAndCorrectSchemaToStringWithPreviousRecordTypeReference() throws IOException {
        String testSchemaJson = "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":\"A\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"B\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"B\",\"fields\":[{\"name\":\"D\"," +
                "\"type\":{\"type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"BD1\",\"type\":\"string\"}," +
                "{\"name\":\"BD2\",\"type\":\"string\"}]}}]}},{\"name\":\"C\",\"type\":{\"type\":\"record\"," +
                "\"name\":\"C\",\"fields\":[{\"name\":\"D\",\"type\":\"D\"}]}}]}}]}";

        String expectedJSON = "{\"type\":\"record\",\"name\":\"etlSchemaBody\",\"fields\":[{\"name\":" +
                "\"A\",\"type\":{\"type\":\"record\",\"name\":\"A\",\"fields\":[{\"name\":\"B\",\"type\":{\"type\":\"" +
                "record\",\"name\":\"B\",\"fields\":[{\"name\":\"D\",\"type\":{\"type\":\"record\",\"name\":\"D\",\"" +
                "fields\":[{\"name\":\"BD1\",\"type\":\"string\"},{\"name\":\"BD2\",\"type\":\"string\"}]}}]}},{\"" +
                "name\":\"C\",\"type\":{\"type\":\"record\",\"name\":\"C\",\"fields\":[{\"name\":\"D\",\"type\":{\"" +
                "type\":\"record\",\"name\":\"D\",\"fields\":[{\"name\":\"BD1\",\"type\":\"string\"}," +
                "{\"name\":\"BD2\",\"type\":\"string\"}]}}]}}]}}]}";

        Schema schema = Schema.parseJson(testSchemaJson);
        Assert.assertEquals(schema.toString(), expectedJSON);
    }
}
