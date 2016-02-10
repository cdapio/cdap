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

package co.cask.cdap.etl.api;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 *
 */
public class Preconditions {

  private final long timeoutSec;
  private final List<Condition> conditions;

  public Preconditions(long timeoutSec, List<Condition> conditions) {
    this.timeoutSec = timeoutSec;
    this.conditions = conditions;
  }

  public Preconditions() {
    this(0, ImmutableList.<Condition>of());
  }

  public long getTimeoutSec() {
    return timeoutSec;
  }

  public List<Condition> getConditions() {
    return conditions;
  }
}
