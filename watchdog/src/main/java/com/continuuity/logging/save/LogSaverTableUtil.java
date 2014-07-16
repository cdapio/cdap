/*
 * Copyright 2012-2014 Continuuity, Inc.
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

package com.continuuity.logging.save;

import com.continuuity.data.DataSetAccessor;
import com.continuuity.logging.LoggingConfiguration;
import com.google.inject.Inject;

/**
 * Helper class for working with the dataset table used by {@link LogSaver}.
 */
public class LogSaverTableUtil extends com.continuuity.data2.dataset.lib.table.MetaTableUtil {
  private static final String TABLE_NAME = LoggingConfiguration.LOG_META_DATA_TABLE;

  @Inject
  public LogSaverTableUtil(DataSetAccessor dataSetAccessor) {
    super(dataSetAccessor);
  }

  @Override
  public String getMetaTableName() {
    return TABLE_NAME;
  }
}
