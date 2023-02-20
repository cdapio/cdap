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

package io.cdap.cdap.etl.spi.relational;

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.engine.sql.StandardSQLCapabilities;
import io.cdap.cdap.etl.api.relational.Capability;
import io.cdap.cdap.etl.api.relational.Expression;
import io.cdap.cdap.etl.api.relational.ExpressionFactory;
import io.cdap.cdap.etl.api.relational.ExpressionFactoryType;
import io.cdap.cdap.etl.api.relational.InvalidExtractableExpression;
import io.cdap.cdap.etl.api.relational.StringExpression;
import io.cdap.cdap.etl.api.relational.StringExpressionFactoryType;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An expression factory which converts a SQL {@link Expression} from one SQL dialect to another on compiling it.
 */
public class SQLDialectConversionExpressionFactory implements ExpressionFactory<String> {

    SQLDialect sourceDialect;
    SQLDialect destinationDialect;
    @Nullable Schema schema;
    SQLDialectConverter converter;

    private static final Set<Capability> CAPABILITIES = Collections.unmodifiableSet(
            new HashSet<Capability>() {{
                add(StringExpressionFactoryType.SQL);
                add(StandardSQLCapabilities.BIGQUERY);
                add(StandardSQLCapabilities.SPARK);
            }}
    );

    public SQLDialectConversionExpressionFactory(SQLDialect src, SQLDialect dest, SQLDialectConverter converter) {
        this(src, dest, null, converter);
    }

    public SQLDialectConversionExpressionFactory(SQLDialect src,
                                                 SQLDialect dest,
                                                 @Nullable Schema schema,
                                                 SQLDialectConverter converter) {
        sourceDialect = src;
        destinationDialect = dest;
        this.schema = schema;
        this.converter = converter;
    }

    /**
     * Gets the expression factory type, which in this case is SQL.
     * @return {@link StringExpressionFactoryType}.SQL.
     */
    @Override
    public ExpressionFactoryType<String> getType() {
        return StringExpressionFactoryType.SQL;
    }

    /**
     * Accepts a SQL string in the source SQL dialect and validates it.
     * If the SQL string is valid, converts it to the destination SQL dialect and wraps it in an {@link Expression}.
     * @param expression A SQL string valid in the source dialect
     * @return Either a valid SQL string expression in the destination dialect or
     * an {@link InvalidExtractableExpression}
     */
    @Override
    public Expression compile(String expression) {
        String compiledSql;
        try {
            compiledSql = converter.convert(expression, sourceDialect, destinationDialect, schema);
        } catch (SQLDialectException exception) {
            return new InvalidExtractableExpression<String>(exception.getMessage());
        }
        
        return new StringExpression(compiledSql);
    }

    /**
     * Get the set of capabilities supported.
     * @return a set of all capabilities supported by this SQL engine.
     */
    @Override
    public Set<Capability> getCapabilities() {
        return CAPABILITIES;
    }

    /**
     * Sets the schema for the expression factory. This is used for validating the SQL expression against the schema
     * and performing the conversion.
     * @param schema A CDAP-format schema.
     */
    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
