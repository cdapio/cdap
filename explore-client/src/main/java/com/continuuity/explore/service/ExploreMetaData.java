package com.continuuity.explore.service;

import java.util.List;

/**
 * Interface for obtaining meta data about explore.
 */
public interface ExploreMetaData {

  public Handle getColumns(String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern)
      throws ExploreException;

  public Handle getSchemas(String catalogName, String schemaName) throws ExploreException;

  public Handle getFunctions(String catalogName, String schemaName, String functionName) throws ExploreException;

  public MetaDataInfo getInfo(String infoName) throws ExploreException;

  public Handle getTables(String catalogName, String schemaName, String tableName, List<String> tableTypes)
      throws ExploreException;

  public Handle getTableTypes() throws ExploreException;

  public Handle getTypeInfo() throws ExploreException;

}
