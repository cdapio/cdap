package com.continuuity.passport.testhelper;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.Constants;
import com.continuuity.passport.core.service.AuthenticatorService;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.service.SecurityService;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.NonceDAO;
import com.continuuity.passport.dal.OrganizationDAO;
import com.continuuity.passport.dal.ProfanityFilter;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.dal.db.AccountDBAccess;
import com.continuuity.passport.dal.db.NonceDBAccess;
import com.continuuity.passport.dal.db.OrganizationDBAccess;
import com.continuuity.passport.dal.db.ProfanityFilterFileAccess;
import com.continuuity.passport.dal.db.VpcDBAccess;
import com.continuuity.passport.http.handlers.AccountHandler;
import com.continuuity.passport.http.handlers.ActivationNonceHandler;
import com.continuuity.passport.http.handlers.OrganizationHandler;
import com.continuuity.passport.http.handlers.SessionNonceHandler;
import com.continuuity.passport.http.handlers.VPCHandler;
import com.continuuity.passport.impl.AuthenticatorServiceImpl;
import com.continuuity.passport.impl.DataManagementServiceImpl;
import com.continuuity.passport.impl.SecuritySeviceImpl;
import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import org.hsqldb.jdbc.pool.JDBCPooledDataSource;

/**
 *
 */
public class MockGuiceModule extends AbstractModule {

  private final String jdbcType;
  private final String connectionString;
  private final String profaneWordsPath;
  private final int port;


  public MockGuiceModule(CConfiguration configuration) {
    jdbcType = configuration.get(Constants.CFG_JDBC_TYPE, Constants.DEFAULT_JDBC_TYPE);
    connectionString = configuration.get(Constants.CFG_JDBC_CONNECTION_STRING,
                                         Constants.DEFAULT_JDBC_CONNECTION_STRING);
    profaneWordsPath = configuration.get(Constants.CFG_PROFANE_WORDS_FILE_PATH,
                                         Constants.DEFAULT_PROFANE_WORDS_FILE_PATH);
    port = configuration.getInt(Constants.CFG_SERVER_PORT, 7777);
  }


  @Override
  protected void configure() {
    Preconditions.checkNotNull(jdbcType, "JDBC type cannot be null");
    Preconditions.checkArgument(jdbcType.equals(Constants.DEFAULT_JDBC_TYPE), "Unsupported JDBC type");

    Preconditions.checkNotNull(connectionString, "Connection String cannot be null");
    Preconditions.checkNotNull(profaneWordsPath, "Profane words path cannot be null"); //TODO: Remove this constraint

    JDBCPooledDataSource jdbcDataSource = new JDBCPooledDataSource();
    System.out.println(connectionString);
    jdbcDataSource.setUrl(connectionString);

    DBConnectionPoolManager connectionPoolManager = new DBConnectionPoolManager(jdbcDataSource, 10);

    bind(DBConnectionPoolManager.class)
      .toInstance(connectionPoolManager);

    bindConstant().annotatedWith(Names.named(Constants.CFG_PROFANE_WORDS_FILE_PATH))
      .to(profaneWordsPath);

    bind(AccountHandler.class);
    bind(VPCHandler.class);
    bind(SessionNonceHandler.class);
    bind(ActivationNonceHandler.class);
    bind(OrganizationHandler.class);

    bind(DataManagementService.class).to(DataManagementServiceImpl.class);
    bind(AuthenticatorService.class).to(AuthenticatorServiceImpl.class);
    bind(SecurityService.class).to(SecuritySeviceImpl.class);

    //Bind Data Access objects
    bind(AccountDAO.class).to(AccountDBAccess.class);
    bind(VpcDAO.class).to(VpcDBAccess.class);
    bind(NonceDAO.class).to(NonceDBAccess.class);
    bind(OrganizationDAO.class).to(OrganizationDBAccess.class);
    bind(ProfanityFilter.class).to(ProfanityFilterFileAccess.class);
  }
}
