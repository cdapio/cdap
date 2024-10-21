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

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A default implementation of Expression that simply stores the expression as a string.
 * The string is assumed to be ANSI SQL compliant.
 * It is the responsibility of the factory creating {@link StringExpression} objects to enforce correctness of
 * the expression.
 */
public final class StringExpression implements ExtractableExpression<String> {
    private final String expression;

    /**
     * Creates a {@link StringExpression} from the specified SQL string.
     * @param expression a String containing the SQL expression.
     */
    public StringExpression(String expression) {
        this.expression = expression;
    }

    /**
     *
     * @return the SQL string contained in the {@link StringExpression} object.
     */
    @Nullable
    @Override
    public String extract() throws InvalidExtractableExpressionException {
        return expression;
    }

    /**
     * A {@link StringExpression} is always assumed to contain valid SQL. It is the responsibility of the creator of the
     * object to ensure correctness of the SQL string.
     * @return This method always returns true.
     */
    @Override
    public boolean isValid() {
        return true;
    }

    /**
     * A {@link StringExpression} is always assumed to contain valid SQL. It is the responsibility of the creator of the
     * object to ensure correctness of the SQL string.
     * @return This method always returns a null string.
     */
    @Override
    public String getValidationError() {
        return null;
    }

    /**
     * Two {@link StringExpression} objects are considered equal if they contain equal SQL expressions.
     * @param obj other object to be compared for equality.
     * @return true iff both objects are {@link StringExpression}s with equal SQL strings.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StringExpression that = (StringExpression) obj;
        return Objects.equals(extract(), that.extract());
    }

    @Override
    public int hashCode() {
        return Objects.hash(extract());
    }
}
