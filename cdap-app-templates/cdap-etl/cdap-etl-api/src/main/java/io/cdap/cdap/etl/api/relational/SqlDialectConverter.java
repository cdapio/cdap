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

import javax.annotation.Nullable;

/**
 * An SPI that declares a service for conversion of a SQL expression from one SQL dialect to another.
 * The {@link SqlDialectConversionExpressionFactory} uses this service to perform the conversion.
 */
public interface SqlDialectConverter {

    /**
     * Returns the name of this SQL dialect converter. The name needs to match with the configuration provided through
     * {@code sqldialect.implementation}.
     */
    String getName();

    /**
     * Validates the specified SQL expression using the schema given for the specified SQL dialect
     *
     * @param expression A SQL expression in the form of a full statement in the SQL dialect specified
     * @param srcDialect The SQL dialect in which the SQL statement is specified
     * @param schema The schema for the table being used in the SQL expression
     * @throws SqlDialectException in case the SQL expression is invalid, or any other error related to the SQL
     * dialect validation is encountered
     */
    void validate(String expression, SqlDialect srcDialect, @Nullable Schema schema) throws SqlDialectException;

    /**
     * Converts the specified SQL expression using the schema given from the specified source SQL dialect to the
     * specified destination SQL dialect
     *
     * @param expression A SQL expression in the form of a full statement in the source SQL dialect specified
     * @param srcDialect The SQL dialect in which the SQL statement is specified
     * @param destDialect The SQL dialect to which the SQL statement is to be converted
     * @param schema The schema for the table being used in the SQL expression
     * @return An String containing the converted SQL expression
     * @throws SqlDialectException in case an error is encountered during SQL dialect conversion is encountered
     */
    String convert(String expression, SqlDialect srcDialect, SqlDialect destDialect, @Nullable Schema schema)
            throws SqlDialectException;
}
