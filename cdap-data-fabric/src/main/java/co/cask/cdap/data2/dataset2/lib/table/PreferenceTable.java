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
 * Preference Table Interface that stores ProgramPreference.
 */
public interface PreferenceTable extends Dataset {

  /**
   * Returns the value stored for a key in a given {@link ProgramRecord}
   * @param program Program
   * @param key Key
   * @return Value
   */
  public String getNote(ProgramRecord program, String key);

  /**
   * Sets the value for a key in a given {@link ProgramRecord}
   * @param program Program
   * @param key Key
   * @param value Value
   */
  public void setNote(ProgramRecord program, String key, String value);

  /**
   * Returns the contents from StateStore for the given {@link ProgramRecord}
   * @param program Program
   * @return Map of contents
   */
  public Map<String, String> getNotes(ProgramRecord program);

  /**
   * Stores the contents in StateStore for the given {@link ProgramRecord}
   * @param program Program
   * @param notes Map of contents
   */
  public void setNotes(ProgramRecord program, Map<String, String> notes);

  /**
   * Delete the contents of the StateStore for the given {@link ProgramRecord}
   * @param program Program
   */
  public void deleteNotes(ProgramRecord program);
}
