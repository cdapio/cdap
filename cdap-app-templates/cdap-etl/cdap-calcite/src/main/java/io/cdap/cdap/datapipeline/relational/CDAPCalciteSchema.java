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
import io.cdap.cdap.etl.api.relational.SQLDialectException;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.MapSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * This class is a representation of the {@link Schema} defined by CDAP in a way compatible with Calcite.
 * It includes the {@code fromCDAPSchema} method to create a Calcite-compatible schema from a CDAP {@link Schema}.
 */
public class CDAPCalciteSchema extends AbstractSchema {
    private final Map<String, Table> tableMap;

    private CDAPCalciteSchema(String tableName, Table table) {
        tableMap = Collections.singletonMap(tableName, table);
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

    public static CDAPCalciteSchema fromCDAPSchema(Schema cdapSchema) {
        // Get list of fields
        List<Schema.Field> cdapFields = cdapSchema.getFields();
        List<RelDataTypeField> calciteFields = getListOfFields(cdapFields);

        CDAPCalciteSchema calciteSchema = new CDAPCalciteSchema(cdapSchema.getRecordName(), createTable(calciteFields));
        return calciteSchema;
    }

    private static List<RelDataTypeField> getListOfFields(List<Schema.Field> cdapFields) {
        List<RelDataTypeField> calciteFields = new ArrayList<>();

        for (int index = 0; index < cdapFields.size(); index++) {
            Schema.Field cdapField = cdapFields.get(index);
            RelDataTypeFieldImpl calciteField = fromCDAPField(cdapField, index);
            calciteFields.add(calciteField);
        }

        return calciteFields;
    }

    private static RelDataTypeFieldImpl fromCDAPField(Schema.Field cdapField, int index) {
        Schema cdapSchema = cdapField.getSchema();
        String fieldName = cdapField.getName();

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
                throw new SQLDialectException("Invalid data type found in schema: " + cdapDatatype);
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
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.DATE);
        } else {
            calciteDatatype = new BasicSqlType(RelDataTypeSystem.DEFAULT, SqlTypeName.VARCHAR);
        }
        return calciteDatatype;
    }
}
