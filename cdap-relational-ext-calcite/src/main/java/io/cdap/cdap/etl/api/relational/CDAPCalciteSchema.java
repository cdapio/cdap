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
import org.apache.calcite.rel.type.*;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CDAPCalciteSchema extends AbstractSchema {
    Map<String, Table> tableMap;

    private CDAPCalciteSchema(String tableName, Table table) {
        tableMap = new HashMap<>();
        tableMap.put(tableName, table);
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

    private static Table createTable(List<RelDataTypeField> fields) {
        Table table = new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
                return new RelRecordType(fields);
            }
        };
        return table;
    }

    public static CDAPCalciteSchema fromCdapSchema(Schema cdapSchema) {
        // Get list of fields
        List<Schema.Field> cdapFields = cdapSchema.getFields();
        List<RelDataTypeField> calciteFields = getListOfFields(cdapFields);

        CDAPCalciteSchema calciteSchema = new CDAPCalciteSchema(cdapSchema.getRecordName(), createTable(calciteFields));
        return calciteSchema;
    }

    private static List<RelDataTypeField> getListOfFields(List<Schema.Field> cdapFields) {
        List<RelDataTypeField> calciteFields = new ArrayList<>();
        int index = 0;

        for (Schema.Field cdapField: cdapFields) {
            RelDataTypeFieldImpl calciteField = fromCdapField(cdapField, index);
            calciteFields.add(calciteField);
            index++;
        }

        return calciteFields;
    }

    private static RelDataTypeFieldImpl fromCdapField(Schema.Field cdapField, int index) {
        Schema cdapSchema = cdapField.getSchema();
        String fieldName = cdapField.getName().toUpperCase();

        RelDataType calciteDatatype = getDatatypeFromSchema(cdapSchema);
        RelDataTypeFieldImpl calciteField = new RelDataTypeFieldImpl(fieldName, index, calciteDatatype);

        return calciteField;
    }

    private static RelDataType getDatatypeFromSchema(Schema cdapSchema) {
        return getDatatypeFromSchema(cdapSchema, false);
    }

    private static RelDataType getDatatypeFromSchema(Schema cdapSchema, boolean isNullable) {
        Schema.Type cdapDatatype = cdapSchema.getType();
        RelDataType calciteDatatype;

        switch (cdapDatatype) {
            case NULL:
                calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.NULL);
                break;
            case BOOLEAN:
                calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BOOLEAN);
                break;
            case INT:
                calciteDatatype = getDatatypeFromInt(cdapSchema);
                break;
            case LONG:
                calciteDatatype = getDatatypeFromLong(cdapSchema);
                break;
            case FLOAT:
                calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.FLOAT);
                break;
            case DOUBLE:
                calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DOUBLE);
                break;
            case BYTES:
                calciteDatatype = getDatatypeFromBytes(cdapSchema);
                break;
            case STRING:
                calciteDatatype = getDatatypeFromString(cdapSchema);
                break;
            case ENUM:
                // TODO: Add implementation
                calciteDatatype = null;
                break;
            case ARRAY:
                calciteDatatype = new ArraySqlType(getDatatypeFromSchema(cdapSchema.getComponentSchema()), isNullable);
                break;
            case MAP:
                Schema cdapKeySchema = cdapSchema.getMapSchema().getKey();
                Schema cdapValueSchema = cdapKeySchema.getMapSchema().getValue();
                calciteDatatype = new MapSqlType(getDatatypeFromSchema(cdapKeySchema),
                        getDatatypeFromSchema(cdapValueSchema), isNullable);
                break;
            case RECORD:
                calciteDatatype = new RelRecordType(getListOfFields(cdapSchema.getFields()));
                break;
            case UNION:
                if (cdapSchema.isNullable()) {
                    calciteDatatype = getDatatypeFromSchema(cdapSchema.getNonNullable(), true);
                } else {
                    // TODO: Add implementation
                    calciteDatatype = null;
                }
                break;
            default:
                // TODO: Throw exception and remove null
                calciteDatatype = null;
        }

        return calciteDatatype;
    }

    private static RelDataType getDatatypeFromInt(Schema cdapSchema) {
        Schema.LogicalType logicalType = cdapSchema.getLogicalType();
        RelDataType calciteDatatype;

        if (logicalType != null) {
            switch (logicalType) {
                case DATE:
                    calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DATE);
                    break;
                case TIME_MILLIS:
                    calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIME);
                    break;
                default:
                    calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);
            }
        } else {
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.INTEGER);
        }
        return calciteDatatype;
    }

    private static RelDataType getDatatypeFromLong(Schema cdapSchema) {
        Schema.LogicalType logicalType = cdapSchema.getLogicalType();
        RelDataType calciteDatatype;

        if (logicalType != null) {
            switch (logicalType) {
                case TIMESTAMP_MILLIS:
                case TIMESTAMP_MICROS:
                    calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIMESTAMP);
                    break;
                case TIME_MICROS:
                    calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.TIME);
                    break;
                default:
                    calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
            }
        } else {
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.BIGINT);
        }
        return calciteDatatype;
    }

    private static RelDataType getDatatypeFromBytes(Schema cdapSchema) {
        Schema.LogicalType logicalType = cdapSchema.getLogicalType();
        RelDataType calciteDatatype;

        if (logicalType == Schema.LogicalType.DECIMAL) {
            int precision = cdapSchema.getPrecision();
            int scale = cdapSchema.getScale();
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DECIMAL, precision, scale);
        } else {
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARBINARY);
        }
        return calciteDatatype;
    }

    private static RelDataType getDatatypeFromString(Schema cdapSchema) {
        Schema.LogicalType logicalType = cdapSchema.getLogicalType();
        RelDataType calciteDatatype;

        if (logicalType == Schema.LogicalType.DATETIME) {
            // TODO: Check this
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DATE);
        } else {
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR);
        }
        return calciteDatatype;
    }
}
