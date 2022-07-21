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

import javax.annotation.Nullable;

/**
 * Invalid Extractable Expression specification
 * @param <T> type of the scalar value contained in this expression
 */
public class InvalidExtractableExpression<T> implements ExtractableExpression<T> {
    String validationError;

    public InvalidExtractableExpression(String validationError) {
        this.validationError = validationError;
    }

    @Override
    public boolean isValid() {
        return false;
    }

    @Override
    public String getValidationError() {
        return validationError;
    }

    @Nullable
    @Override
    public T extract() {
        return null;
    }
}
