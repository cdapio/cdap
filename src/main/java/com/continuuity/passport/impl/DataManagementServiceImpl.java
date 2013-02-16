package com.continuuity.passport.impl;

import com.continuuity.passport.core.exceptions.ConfigurationException;
import com.continuuity.passport.core.exceptions.RetryException;
import com.continuuity.passport.core.meta.Account;
import com.continuuity.passport.core.meta.AccountSecurity;
import com.continuuity.passport.core.meta.Component;
import com.continuuity.passport.core.meta.Credentials;
import com.continuuity.passport.core.meta.VPC;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.status.Status;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.dal.db.AccountDBAccess;
import com.continuuity.passport.dal.db.VpcDBAccess;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DataManagementServiceImpl implements DataManagementService {


  private static DataManagementService service = null;

  private AccountDAO accountDAO = null;

  private VpcDAO vpcDao = null;


  private DataManagementServiceImpl() {
    accountDAO = new AccountDBAccess();
    Map<String,String> config = new HashMap<String,String>();
    config.put("jdbcType","mysql");
   // config.put("connectionString","jdbc:mysql://a101.dev.sl:3306/continuuity?user=passport_user");
    config.put("connectionString","jdbc:mysql://localhost/continuuity?user=passport_user");
    accountDAO.configure(config);

    vpcDao = new VpcDBAccess();
    vpcDao.configure(config);
  }

  /**
   * Register an {@code Account} in the system
   *
   * @param account Account information
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public long registerAccount(Account account) throws RuntimeException {
    if (accountDAO ==null) {
      throw new RuntimeException("Could not init data access Object");

    }
    try {
      return accountDAO.createAccount(account);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e.getMessage());
    }
  }

  @Override
  public Status confirmRegistration(AccountSecurity account) throws RuntimeException {

    if (accountDAO ==null) {
      throw new RuntimeException("Could not init data access Object");

    }
    try {
      accountDAO.confirmRegistration(account);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e.getMessage());
    }
    return null;
  }

  /**
   * Register a component with the account- Example: register VPC, Register DataSet
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status registerComponents(String accountId, Credentials credentials, Component component)
                                                                                    throws RetryException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Unregister a {@code Component} in the system
   *
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status unRegisterComponent(String accountId, Credentials credentials, Component component)
                                                                                    throws RetryException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * Delete an {@code Account} in the system
   *
   * @param accountId   account to be deleted
   * @param credentials credentials of the owner of the account
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status deleteAccount(String accountId, Credentials credentials) throws RetryException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * @param accountId
   * @param credentials
   * @param component
   * @return Instance of {@code Status}
   * @throws RuntimeException
   */
  @Override
  public Status updateComponent(String accountId, Credentials credentials, Component component)
                                                                          throws RetryException {
    return null;  //To change body of implemented methods use File | Settings | File Templates.
  }

  /**
   * GetAccount object
   *
   * @param accountId Id of the account
   * @return Instance of {@code Account}
   */
  @Override
  public Account getAccount(int accountId) throws RuntimeException {

    Account account = null;
    if (accountDAO ==null) {
      throw new RuntimeException("Could not init data access Object");
    }
    try {
     account= accountDAO.getAccount(accountId);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e.getMessage());
    }
    return account;
  }

  @Override
  public List<VPC> getVPC(int accountId) {
    List<VPC> vpcs;
    if(vpcDao == null) {
      throw new RuntimeException("Could not initialize data access object");
    }
    try {
       vpcs = vpcDao.getVPC(accountId);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e.getMessage());
    }
    return vpcs;
  }

  /**
   * Get VPC List based on the ApiKey
   *
   * @param apiKey apiKey of the account
   * @return List of {@code VPC}
   */
  @Override
  public List<VPC> getVPC(String apiKey) {
    List<VPC> vpcs;
    if(vpcDao == null) {
      throw new RuntimeException("Could not initialize data access object");
    }
    try {
      vpcs = vpcDao.getVPC(apiKey);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e.getMessage());
    }
    return vpcs;

  }

  /**
   * Update account with passed Params
   *
   * @param accountId accountId
   * @param params    Map<"keyName", "value">
   */
  @Override
  public void updateAccount(int accountId, Map<String, Object> params) throws RuntimeException {

    if (accountDAO ==null) {
      throw new RuntimeException("Could not init data access Object");
    }
    try {
      accountDAO.updateAccount(accountId,params);
    } catch (Exception e) {
      throw new RuntimeException(e.getMessage());
    }

  }

  public long addVPC(int accountId, VPC vpc) throws RuntimeException {
    if(vpcDao == null) {
      throw new RuntimeException("Could not initialize data access object");
    }
    try {
     return vpcDao.addVPC(accountId, vpc);
    } catch (ConfigurationException e) {
      throw new RuntimeException(e.getMessage());
    }
  }


  public static DataManagementService getInstance(){
    if (service == null ){
      service = new DataManagementServiceImpl();
    }
    return service;
  }

}
