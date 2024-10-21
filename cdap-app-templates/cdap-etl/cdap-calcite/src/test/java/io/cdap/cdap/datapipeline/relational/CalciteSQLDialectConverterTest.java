/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.datapipeline.relational;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.relational.SQLDialect;
import io.cdap.cdap.etl.api.relational.SQLDialectException;
import org.junit.BeforeClass;
import org.junit.Test;

public class CalciteSQLDialectConverterTest {

    private static Schema schema;
    private static CalciteSQLDialectConverter converter;

    @BeforeClass
    public static void setup() throws Exception {
        /*
        Schema:
            emp_id string,
            mgr_dept_cd string,
            dept_cd string,
            level decimal(3, 0),
            salary double,
            start_dttm timestamp,
            end_dttm timestamp
         */
        String jsonSchema = "{\"name\":\"etlSchemaBody\",\"type\":\"record\",\"fields\":" +
                "[{\"name\":\"emp_id\",\"type\":\"string\"}," +
                "{\"name\":\"mgr_dept_cd\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"dept_cd\",\"type\":[\"string\",\"null\"]}," +
                "{\"name\":\"level\",\"type\":" +
                "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":3,\"scale\":0}}," +
                "{\"name\":\"salary\",\"type\":\"double\"}," +
                "{\"name\":\"start_dttm\",\"type\":" +
                "[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]}," +
                "{\"name\":\"end_dttm\",\"type\":" +
                "[{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},\"null\"]}]}";

        schema = Schema.parseJson(jsonSchema);
        converter = new CalciteSQLDialectConverter();
    }

    @Test
    public void testCharLiteralAliasBQ() {
        String query = "SELECT 'abc' AS 'a' FROM etlSchemaBody";
        converter.validate(query, SQLDialect.BIGQUERYSQL, schema);
    }

    @Test(expected = SQLDialectException.class)
    public void testCharLiteralAliasPostgres() {
        String query = "SELECT 'abc' AS 'a' FROM etlSchemaBody";
        converter.validate(query, SQLDialect.POSTGRESQL, schema);
    }

    @Test
    public void testGroupByAliasBQ() {
        String query = "SELECT avg(salary), level AS designation FROM etlSchemaBody GROUP BY designation";
        converter.validate(query, SQLDialect.BIGQUERYSQL, schema);
    }

    @Test(expected = SQLDialectException.class)
    public void testGroupByAliasPostgres() {
        String query = "SELECT avg(salary), level AS designation FROM etlSchemaBody GROUP BY designation";
        converter.validate(query, SQLDialect.POSTGRESQL, schema);
    }
}
