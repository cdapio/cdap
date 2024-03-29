/*
 * Copyright © 2015-2016 Cask Data, Inc.
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

package io.cdap.cdap.test.app;

import com.google.common.base.Objects;

/**
 * App that creates the RecordDataset of {@link DatasetDeployApp} with a compatible record class.
 */
public class CompatibleDatasetDeployApp extends DatasetDeployApp {

  @Override
  protected Class<?> getRecordClass() {
    return CompatibleRecord.class;
  }

  public static final class CompatibleRecord {
    private final String id;
    private final String firstName;
    private final String lastName;

    public CompatibleRecord(String id, String firstName, String lastName) {
      this.id = id;
      this.firstName = firstName;
      this.lastName = lastName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      CompatibleRecord that = (CompatibleRecord) o;

      return Objects.equal(this.id, that.id)
          && Objects.equal(this.firstName, that.firstName)
          && Objects.equal(this.lastName, that.lastName);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, firstName, lastName);
    }
  }
}
