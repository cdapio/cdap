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
import io.cdap.cdap.etl.spi.relational.SQLDialect;
import io.cdap.cdap.etl.spi.relational.SQLDialectConverter;
import io.cdap.cdap.etl.spi.relational.SQLDialectException;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.BigQuerySqlDialect;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.ValidationException;

import javax.annotation.Nullable;

public class CalciteSQLDialectConverter implements SQLDialectConverter {
    public static final String CALCITE_IMPL_NAME = "calcite";

    @Override
    public String getName() {
        return CALCITE_IMPL_NAME;
    }

    @Override
    public void validate(String expression, SQLDialect srcDialect, @Nullable Schema schema) throws SQLDialectException {
        PlannerImpl planner = getPlanner(srcDialect, schema);

        try {
            planner.validate(planner.parse(expression));
        } catch (SqlParseException | ValidationException exception) {
            throw new SQLDialectException(exception.getMessage(), exception);
        }
    }

    @Override
    public String convert(String expression, SQLDialect srcDialect, SQLDialect destDialect, @Nullable Schema schema)
            throws SQLDialectException {
        PlannerImpl planner = getPlanner(srcDialect, schema);

        SqlNode node;
        try {
            node = planner.parse(expression);
        } catch (SqlParseException exception) {
            throw new SQLDialectException(exception.getMessage(), exception);
        }
        return node.toSqlString(getDialectObject(destDialect)).getSql();
    }

    private PlannerImpl getPlanner(SQLDialect srcDialect, @Nullable Schema schema) throws SQLDialectException {
        if (schema == null) {
            throw new SQLDialectException("Calcite-based Sql parser / validator requires an input schema, none found");
        }

        CDAPCalciteSchema calciteSchema = CDAPCalciteSchema.fromCDAPSchema(schema);
        SchemaPlus schemaPlus = Frameworks.createRootSchema(true);
        schemaPlus = schemaPlus.add(schema.getRecordName(), calciteSchema);

        SqlParser.Config parserConfig = SqlParser.config()
                .withCaseSensitive(false)
                .withConformance(getConformanceFromDialect(srcDialect));

        SqlValidator.Config validatorConfig = SqlValidator.Config.DEFAULT
                .withConformance(getConformanceFromDialect(srcDialect));

        final FrameworkConfig config = Frameworks.newConfigBuilder()
                .defaultSchema(schemaPlus)
                .parserConfig(parserConfig)
                .sqlValidatorConfig(validatorConfig)
                .build();
        PlannerImpl planner = new PlannerImpl(config);

        return planner;
    }

    private SqlConformance getConformanceFromDialect(SQLDialect dialect) throws SQLDialectException {
        switch (dialect) {
            case BIGQUERYSQL:
                return SqlConformanceEnum.BIG_QUERY;
            case POSTGRESQL:
            case SPARKSQL:
                // Note for SPARKSQL:
                // It has to be a SqlConformanceEnum as CalciteConnectionConfigImpl.conformance() expects one
                // And a SPARK enum doesn't exist
                return SqlConformanceEnum.DEFAULT;
            default:
                throw new SQLDialectException("SQL dialect " + dialect + " not found");
        }
    }

    private org.apache.calcite.sql.SqlDialect getDialectObject(SQLDialect dialect) throws SQLDialectException {
        switch (dialect) {
            case BIGQUERYSQL:
                return new BigQuerySqlDialect(BigQuerySqlDialect.DEFAULT_CONTEXT);
            case POSTGRESQL:
                return new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT);
            case SPARKSQL:
                return new SparkSqlDialect(SparkSqlDialect.DEFAULT_CONTEXT);
            default:
                throw new SQLDialectException("SQL dialect " + dialect + " not found");
        }
    }
}
