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

package co.cask.cdap.explore.service;

/**
 * Metadata information about Explore service.
 */
public class MetaDataInfo {

  /**
   * Information types.
   */
  public enum InfoType {
    MAX_DRIVER_CONNECTIONS(null),
    MAX_CONCURRENT_ACTIVITIES(null),
    DATA_SOURCE_NAME(null),
    FETCH_DIRECTION(null),
    SERVER_NAME(null),
    SEARCH_PATTERN_ESCAPE(null),
    DBMS_NAME(new MetaDataInfo("CDAP")),
    DBMS_VER(new MetaDataInfo("2.4.0")),
    ACCESSIBLE_TABLES(null),
    ACCESSIBLE_PROCEDURES(null),
    CURSOR_COMMIT_BEHAVIOR(null),
    DATA_SOURCE_READ_ONLY(null),
    DEFAULT_TXN_ISOLATION(null),
    IDENTIFIER_CASE(null),
    IDENTIFIER_QUOTE_CHAR(null),
    MAX_COLUMN_NAME_LEN(null),
    MAX_CURSOR_NAME_LEN(null),
    MAX_SCHEMA_NAME_LEN(null),
    MAX_CATALOG_NAME_LEN(null),
    MAX_TABLE_NAME_LEN(null),
    SCROLL_CONCURRENCY(null),
    TXN_CAPABLE(null),
    USER_NAME(null),
    TXN_ISOLATION_OPTION(null),
    INTEGRITY(null),
    GETDATA_EXTENSIONS(null),
    NULL_COLLATION(null),
    ALTER_TABLE(null),
    ORDER_BY_COLUMNS_IN_SELECT(null),
    SPECIAL_CHARACTERS(null),
    MAX_COLUMNS_IN_GROUP_BY(null),
    MAX_COLUMNS_IN_INDEX(null),
    MAX_COLUMNS_IN_ORDER_BY(null),
    MAX_COLUMNS_IN_SELECT(null),
    MAX_COLUMNS_IN_TABLE(null),
    MAX_INDEX_SIZE(null),
    MAX_ROW_SIZE(null),
    MAX_STATEMENT_LEN(null),
    MAX_TABLES_IN_SELECT(null),
    MAX_USER_NAME_LEN(null),
    OJ_CAPABILITIES(null),

    XOPEN_CLI_YEAR(null),
    CURSOR_SENSITIVITY(null),
    DESCRIBE_PARAMETER(null),
    CATALOG_NAME(null),
    COLLATION_SEQ(null),
    MAX_IDENTIFIER_LEN(null);

    private final MetaDataInfo defaultValue;

    private InfoType(MetaDataInfo defaultValue) {
      this.defaultValue = defaultValue;
    }

    public static InfoType fromString(String str) {
      for (InfoType infoType : InfoType.values()) {
        if (infoType.name().equals(str)) {
          return infoType;
        }
      }
      return null;
    }

    public MetaDataInfo getDefaultValue() {
      return defaultValue;
    }
  }

  private String stringValue = null;
  private short shortValue;
  private int intValue;
  private long longValue;

  public MetaDataInfo(String stringValue, short shortValue, int intValue, long longValue) {
    this.stringValue = stringValue;
    this.shortValue = shortValue;
    this.intValue = intValue;
    this.longValue = longValue;
  }

  public MetaDataInfo(String stringValue) {
    this.stringValue = stringValue;
  }

  public MetaDataInfo(short shortValue) {
    this.shortValue = shortValue;
  }

  public MetaDataInfo(int intValue) {
    this.intValue = intValue;
  }

  public MetaDataInfo(long longValue) {
    this.longValue = longValue;
  }

  public String getStringValue() {
    return stringValue;
  }

  public short getShortValue() {
    return shortValue;
  }

  public int getIntValue() {
    return intValue;
  }

  public long getLongValue() {
    return longValue;
  }

}
