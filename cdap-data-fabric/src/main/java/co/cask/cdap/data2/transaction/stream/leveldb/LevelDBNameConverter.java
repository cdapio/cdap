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

package co.cask.cdap.data2.transaction.stream.leveldb;


import co.cask.cdap.data2.util.TableId;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Utility Class for LevelDB Table names.
 */
public class LevelDBNameConverter {
  public static TableId from(String levelDBTableName) {
    Preconditions.checkArgument(levelDBTableName != null, "Table name should not be null.");
    // remove table-prefix
    String[] tablePrefixSplit = levelDBTableName.split("_");
    Preconditions.checkArgument(tablePrefixSplit.length > 1, "Missing table-prefix");
    String[] tableNameParts = tablePrefixSplit[1].split("\\.", 2);
    Preconditions.checkArgument(tableNameParts.length > 1, "Missing namespace or tableName");
    return TableId.from(tableNameParts[0], tableNameParts[1]);
  }
}
