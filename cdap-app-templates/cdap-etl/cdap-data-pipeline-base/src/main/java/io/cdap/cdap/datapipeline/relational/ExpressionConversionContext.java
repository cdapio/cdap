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
import io.cdap.cdap.etl.api.relational.SQLDialectConverter;

/**
 * A class that provides a Context object to facilitate working with {@link SQLDialectConversionExpressionFactory}
 * objects with specific SQL dialects
 */
public class ExpressionConversionContext {

    SQLDialectConverter converter;

    public ExpressionConversionContext() {
        converter = new CalciteSQLDialectConverter();
    }

    /**
     * Returns a conversion factory initialized with the source and destination SQL dialects specified and
     * an optional schema
     * @param src The SQL dialect in which expressions will be specified
     * @param dest The SQL dialect in which expressions are to be output after conversion
     * @param schema The schema of the table on which the SQL operation is performed
     * @return An expression factory that will accept SQL expressions in the source dialect and convert them into
     * expressions in the destination dialect after validation, taking the schema into account
     */
    public SQLDialectConversionExpressionFactory getConversionFactory(
            SQLDialect src, SQLDialect dest, Schema schema) {
        return new SQLDialectConversionExpressionFactory(src, dest, schema, converter);
    }
}
