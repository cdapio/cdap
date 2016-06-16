/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.etl.api;

import java.util.ArrayList;
import java.util.List;

/**
 * Join Row which has all the records to be combined together
 */
public class JoinResult {

  private List<JoinElement> joinRow;

  /**
   * sets list of join elements
   * @param joinRow list of {@link JoinElement}
   */
  public JoinResult(List<JoinElement> joinRow) {
    this.joinRow = new ArrayList<>(joinRow);
  }

  /**
   * Returns list of {@link JoinElement}
   * @return
   */
  public List<JoinElement> getJoinResult() {
    return joinRow;
  }

}
