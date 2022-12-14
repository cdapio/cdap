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

package io.cdap.cdap.etl.api.relational;

import io.cdap.cdap.api.data.schema.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class CDAPCalciteSchemaTest {

    @Test
    public void testSchemaCreation1() throws IOException {

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

        Schema schema = Schema.parseJson(jsonSchema);
        CDAPCalciteSchema calciteSchema = CDAPCalciteSchema.fromCdapSchema(schema);
        Table table = calciteSchema.getTable("etlSchemaBody");
        RelDataType recordDataType = table.getRowType(new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT));

        // Number of fields
        Assert.assertEquals(schema.getFields().size(), recordDataType.getFieldList().size());

        // Field 1: emp_id
        assertFieldType(recordDataType, "emp_id", SqlTypeName.VARCHAR);

        // Field 2: mgr_dept_cd
        assertFieldType(recordDataType, "mgr_dept_cd", SqlTypeName.VARCHAR);

        // Field 3: dept_cd
        assertFieldType(recordDataType, "dept_cd", SqlTypeName.VARCHAR);

        // Field 4: level
        assertFieldType(recordDataType, "level", SqlTypeName.DECIMAL);
        Assert.assertEquals(recordDataType.getField("level", false, true).getType().getPrecision(), 3);
        Assert.assertEquals(recordDataType.getField("level", false, true).getType().getScale(), 0);

        // Field 5: salary
        assertFieldType(recordDataType, "salary", SqlTypeName.DOUBLE);

        // Field 6: start_dttm
        assertFieldType(recordDataType, "start_dttm", SqlTypeName.TIMESTAMP);

        // Field 7: end_dttm
        assertFieldType(recordDataType, "end_dttm", SqlTypeName.TIMESTAMP);
    }

    private void assertFieldType(RelDataType recordDataType, String fieldName, SqlTypeName type) {
        Assert.assertEquals(recordDataType.getField(fieldName, false, false).getType().getSqlTypeName(), type);
    }
}
