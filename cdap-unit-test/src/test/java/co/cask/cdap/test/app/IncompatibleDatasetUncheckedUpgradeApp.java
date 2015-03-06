/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.test.app;

import com.google.common.base.Objects;

/**
 *
 */
public class IncompatibleDatasetUncheckedUpgradeApp extends DatasetUncheckedUpgradeApp {

  @Override
  protected Class<?> getRecordClass() {
    return Record.class;
  }

  public static final class Record {
    private final long id;
    private final String firstName;
    private final boolean alive;

    public Record(long id, String firstName, boolean alive) {
      this.id = id;
      this.firstName = firstName;
      this.alive = alive;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Record that = (Record) o;

      return Objects.equal(this.id, that.id) &&
        Objects.equal(this.firstName, that.firstName) &&
        Objects.equal(this.alive, that.alive);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, firstName, alive);
    }
  }
}
