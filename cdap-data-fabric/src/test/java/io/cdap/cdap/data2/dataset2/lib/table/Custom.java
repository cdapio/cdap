/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.data2.dataset2.lib.table;

import java.util.ArrayList;

public final class Custom {
  int i;
  ArrayList<String> sl;
  Custom(int i, ArrayList<String> sl) {
    this.i = i;
    this.sl = sl;
  }
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }
    if (this.i != ((Custom) o).i) {
      return false;
    }
    if (this.sl == null) {
      return ((Custom) o).sl == null;
    }
    return this.sl.equals(((Custom) o).sl);
  }
  @Override
  public int hashCode() {
    return 31 * i + (sl != null ? sl.hashCode() : 0);
  }
}
