/*
 * Copyright © 2014-2022 Cask Data, Inc.
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

package io.cdap.cdap.proto.artifact;

import io.cdap.cdap.api.annotation.Beta;

import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Represents the change summary for an update of the application
 */
@Beta
public class ChangeSummary {
    @Nullable
    protected final String description;

    public ChangeSummary(@Nullable String description) {
        this.description = description;
    }

    @Nullable
    public String getDescription() {
        return description == null ? null : description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ChangeSummary that = (ChangeSummary) o;

        return Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(description);
    }

    @Override
    public String toString() {
        return "ChangeSummary{" +
                "description='" + description +
                '}';
    }
}
