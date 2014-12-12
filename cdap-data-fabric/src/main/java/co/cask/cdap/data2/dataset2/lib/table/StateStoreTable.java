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

import co.cask.cdap.api.dataset.Dataset;
import co.cask.cdap.proto.ProgramRecord;

import java.util.Map;

/**
 * StateStore Table Interface that stores a Program's state.
 */
public interface StateStoreTable extends Dataset {

  /**
   * Returns the contents from StateStore for the given {@link ProgramRecord}
   * @param program Program
   * @return Map of contents
   */
  public Map<String, String> getState(ProgramRecord program);

  /**
   * Stores the contents in StateStore for the given {@link ProgramRecord}
   * @param program Program
   * @param state Map of contents
   */
  public void saveState(ProgramRecord program, Map<String, String> state);

  /**
   * Delete the contents of the StateStore for the given {@link ProgramRecord}
   * @param program Program
   */
  public void deleteState(ProgramRecord program);
}
