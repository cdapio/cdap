/*
 * Copyright (c) 2012 Continuuity Inc. All rights reserved.
 */
package com.continuuity.data.runtime;

import java.util.Properties;

import com.continuuity.data.engine.hypersql.HyperSQLColumnarTableHandle;
import com.continuuity.data.engine.hypersql.HyperSQLOVCTableHandle;
import com.continuuity.data.engine.memory.oracle.MemoryStrictlyMonotonicTimeOracle;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.data.operation.executor.omid.OmidTransactionalOperationExecutor;
import com.continuuity.data.operation.executor.omid.TimestampOracle;
import com.continuuity.data.operation.executor.omid.TransactionOracle;
import com.continuuity.data.operation.executor.omid.memory.MemoryOracle;
import com.continuuity.data.table.ColumnarTableHandle;
import com.continuuity.data.table.OVCTableHandle;
import com.google.inject.AbstractModule;
import com.google.inject.Singleton;
import com.google.inject.name.Names;

/**
 * DataFabricLocalModule defines the Local/HyperSQL bindings for the data fabric.
 */
public class DataFabricLocalModule extends AbstractModule {

  private final String hyperSqlJDCBString;
  private final Properties hyperSqlProperties;
  
  private static final Properties DEFAULT_PROPERTIES;
  static {
    DEFAULT_PROPERTIES = new Properties();
    DEFAULT_PROPERTIES.setProperty("user", "SA");
    DEFAULT_PROPERTIES.setProperty("password", "");
  }
  
  public DataFabricLocalModule() {
    this("jdbc:hsqldb:mem:dflmmem", DEFAULT_PROPERTIES);
  }
  
  public DataFabricLocalModule(String hyperSqlJDBCString) {
    this(hyperSqlJDBCString, DEFAULT_PROPERTIES);
  }
  
  public DataFabricLocalModule(String hyperSqlJDBCString,
      Properties hyperSqlProperties) {
    this.hyperSqlJDCBString = hyperSqlJDBCString;
    this.hyperSqlProperties = hyperSqlProperties;
  }

  @Override
  public void configure() {

    // Load any necessary drivers
    loadHsqlDriver();
    
    // Bind our implementations

    // There is only one timestamp oracle for the whole system
    bind(TimestampOracle.class).
        to(MemoryStrictlyMonotonicTimeOracle.class).in(Singleton.class);

    bind(TransactionOracle.class).to(MemoryOracle.class);

    bind(OVCTableHandle.class).to(HyperSQLOVCTableHandle.class);
    bind(ColumnarTableHandle.class).to(HyperSQLColumnarTableHandle.class);
    bind(OperationExecutor.class).
        to(OmidTransactionalOperationExecutor.class).in(Singleton.class);
    
    // Bind named fields
    
    bind(String.class)
        .annotatedWith(Names.named("HyperSQLOVCTableHandleJDBCString"))
        .toInstance(hyperSqlJDCBString);
    bind(Properties.class)
        .annotatedWith(Names.named("HyperSQLOVCTableHandleProperties"))
        .toInstance(hyperSqlProperties);

  }

  private void loadHsqlDriver() {
    try {
      Class.forName("org.hsqldb.jdbcDriver");
    } catch (Exception e) {
      System.err.println("ERROR: failed to load HSQLDB JDBC driver.");
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }
} // end of GatewayProductionModule
