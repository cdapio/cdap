package com.continuuity.explore.service;

/**
 *
 */
public class MetaDataInfo {

  /**
   *
   */
  public enum InfoType {
    MAX_DRIVER_CONNECTIONS,
    MAX_CONCURRENT_ACTIVITIES,
    DATA_SOURCE_NAME,
    FETCH_DIRECTION,
    SERVER_NAME,
    SEARCH_PATTERN_ESCAPE,
    DBMS_NAME,
    DBMS_VER,
    ACCESSIBLE_TABLES,
    ACCESSIBLE_PROCEDURES,
    CURSOR_COMMIT_BEHAVIOR,
    DATA_SOURCE_READ_ONLY,
    DEFAULT_TXN_ISOLATION,
    IDENTIFIER_CASE,
    IDENTIFIER_QUOTE_CHAR,
    MAX_COLUMN_NAME_LEN,
    MAX_CURSOR_NAME_LEN,
    MAX_SCHEMA_NAME_LEN,
    MAX_CATALOG_NAME_LEN,
    MAX_TABLE_NAME_LEN,
    SCROLL_CONCURRENCY,
    TXN_CAPABLE,
    USER_NAME,
    TXN_ISOLATION_OPTION,
    INTEGRITY,
    GETDATA_EXTENSIONS,
    NULL_COLLATION,
    ALTER_TABLE,
    ORDER_BY_COLUMNS_IN_SELECT,
    SPECIAL_CHARACTERS,
    MAX_COLUMNS_IN_GROUP_BY,
    MAX_COLUMNS_IN_INDEX,
    MAX_COLUMNS_IN_ORDER_BY,
    MAX_COLUMNS_IN_SELECT,
    MAX_COLUMNS_IN_TABLE,
    MAX_INDEX_SIZE,
    MAX_ROW_SIZE,
    MAX_STATEMENT_LEN,
    MAX_TABLES_IN_SELECT,
    MAX_USER_NAME_LEN,
    OJ_CAPABILITIES,

    XOPEN_CLI_YEAR,
    CURSOR_SENSITIVITY,
    DESCRIBE_PARAMETER,
    CATALOG_NAME,
    COLLATION_SEQ,
    MAX_IDENTIFIER_LEN;

    public static InfoType fromString(String str) {
      for (InfoType infoType : InfoType.values()) {
        if (infoType.name().equals(str)) {
          return infoType;
        }
      }
      return null;
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
