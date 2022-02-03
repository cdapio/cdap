/*
 * Copyright © 2022 Cask Data, Inc.
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

/**
 * Specifies how a String Expression is.
 */
public class StringExpression implements Expression {
    private final String expression;

    public StringExpression(String expression) {
        this.expression = expression;
    }

    public String getExpression() {
        return expression;
    }

    /**
     * A {@link StringExpression} is always assumed to be valid. It is the responsibility of the creator of the
     * object to ensure correctness of the string expression.
     * @return This method always returns true.
     */
    @Override
    public boolean isValid() {
        return true;
    }

    /**
     * A {@link StringExpression} is always assumed to be valid. It is the responsibility of the creator of the
     * object to ensure correctness of the string expression.
     * @return This method always returns a null string.
     */
    @Override
    public String getValidationError() {
        return null;
    }
}
