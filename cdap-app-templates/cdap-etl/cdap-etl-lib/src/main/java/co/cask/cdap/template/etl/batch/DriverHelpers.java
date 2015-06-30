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

package co.cask.cdap.template.etl.batch;

import co.cask.cdap.template.etl.common.JDBCDriverShim;

import java.sql.Driver;
import javax.annotation.Nullable;

/**
 * Methods for initializing, validating, and destroying a DB source or sink.
 */

public class DriverHelpers {
  @Nullable
  public JDBCDriverShim driverShim;

  @Nullable
  public Class<? extends Driver> driverClass;

  public DriverHelpers(JDBCDriverShim driverShim, Class<? extends Driver> driverClass) {
    this.driverShim = driverShim;
    this.driverClass = driverClass;
  }
}
