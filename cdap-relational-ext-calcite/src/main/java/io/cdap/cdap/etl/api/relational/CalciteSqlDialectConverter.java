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
import io.cdap.cdap.common.conf.Constants;
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

public class CalciteSqlDialectConverter implements SqlDialectConverter {

    @Override
    public String getName() {
        return Constants.SqlDialectConversion.CALCITE_IMPL_NAME;
    }

    @Override
    public void validate(String expression, SqlDialect srcDialect, @Nullable Schema schema) throws SqlDialectException {
        PlannerImpl planner = getPlanner(srcDialect, schema);

        try {
            planner.validate(planner.parse(expression));
        } catch (SqlParseException | ValidationException exception) {
            throw new SqlDialectException(exception.getMessage(), exception);
        }
    }

    @Override
    public String convert(String expression, SqlDialect srcDialect, SqlDialect destDialect, @Nullable Schema schema)
            throws SqlDialectException {
        PlannerImpl planner = getPlanner(srcDialect, schema);

        SqlNode node;
        try {
            node = planner.parse(expression);
        } catch (SqlParseException exception) {
            throw new SqlDialectException(exception.getMessage(), exception);
        }
        return node.toSqlString(getDialectObject(destDialect)).getSql();
    }

    private PlannerImpl getPlanner(SqlDialect srcDialect, @Nullable Schema schema) throws SqlDialectException {
        if (schema == null) {
            throw new SqlDialectException("Calcite-based Sql parser / validator requires an input schema, none found");
        }

        CDAPCalciteSchema calciteSchema = CDAPCalciteSchema.fromCdapSchema(schema);
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

    private SqlConformance getConformanceFromDialect(SqlDialect dialect) throws SqlDialectException {
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
                throw new SqlDialectException("SQL dialect " + dialect + " not found");
        }
    }

    private org.apache.calcite.sql.SqlDialect getDialectObject(SqlDialect dialect) throws SqlDialectException {
        switch (dialect) {
            case BIGQUERYSQL:
                return new BigQuerySqlDialect(BigQuerySqlDialect.DEFAULT_CONTEXT);
            case POSTGRESQL:
                return new PostgresqlSqlDialect(PostgresqlSqlDialect.DEFAULT_CONTEXT);
            case SPARKSQL:
                return new SparkSqlDialect(SparkSqlDialect.DEFAULT_CONTEXT);
            default:
                throw new SqlDialectException("SQL dialect " + dialect + " not found");
        }
    }
}
