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

package io.cdap.cdap.operations.cdap;

import io.cdap.cdap.operations.OperationalStats;

/**
 * {@link OperationalStats} representing CDAP information.
 */
public class CDAPInfo extends AbstractCDAPStats implements CDAPInfoMXBean {

  private final long startTime;

  public CDAPInfo() {
    this.startTime = System.currentTimeMillis();
  }

  @Override
  public long getUptime() {
    return System.currentTimeMillis() - startTime;
  }

  @Override
  public String getStatType() {
    return "info";
  }
}
