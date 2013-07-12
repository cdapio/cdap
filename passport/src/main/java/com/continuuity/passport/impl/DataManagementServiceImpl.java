/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.impl;

import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.AccountNotFoundException;
import com.continuuity.passport.core.exceptions.OrganizationAlreadyExistsException;
import com.continuuity.passport.core.exceptions.OrganizationNotFoundException;
import com.continuuity.passport.core.exceptions.VPCNotFoundException;
import com.continuuity.passport.core.security.Credentials;
import com.continuuity.passport.core.service.DataManagementService;
import com.continuuity.passport.core.status.Status;
import com.continuuity.passport.dal.AccountDAO;
import com.continuuity.passport.dal.OrganizationDAO;
import com.continuuity.passport.dal.ProfanityFilter;
import com.continuuity.passport.dal.VpcDAO;
import com.continuuity.passport.meta.Account;
import com.continuuity.passport.meta.Component;
import com.continuuity.passport.meta.Organization;
import com.continuuity.passport.meta.RolesAccounts;
import com.continuuity.passport.meta.VPC;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 * Implementation for Data-management service.
 */
public class DataManagementServiceImpl implements DataManagementService {
  private final AccountDAO accountDAO;
  private final VpcDAO vpcDao;
  private final ProfanityFilter profanityFilter;
  private final OrganizationDAO organizationDAO;

  @Inject
  public DataManagementServiceImpl(AccountDAO accountDAO, VpcDAO vpcDAO, OrganizationDAO organizationDAO,
                                   ProfanityFilter profanityFilter) {
    this.accountDAO = accountDAO;
    this.vpcDao = vpcDAO;
    this.organizationDAO = organizationDAO;
    this.profanityFilter = profanityFilter;
  }

  @Override
  public Account registerAccount(Account account) throws AccountAlreadyExistsException {
    return accountDAO.createAccount(account);
  }

  @Override
  public void confirmRegistration(Account account, String password) {
    accountDAO.confirmRegistration(account, password);
  }

  @Override
  public void confirmDownload(int accountId) {
    accountDAO.confirmDownload(accountId);
  }

  @Override
  public void confirmPayment(int accountId, String paymentId) {
    accountDAO.confirmPayment(accountId, paymentId);
  }


  @Override
  public Status registerComponents(String accountId, Credentials credentials, Component component) {
    throw new UnsupportedOperationException("This method is not implemented yet.");
  }


  @Override
  public Status unRegisterComponent(String accountId, Credentials credentials, Component component) {
    throw new UnsupportedOperationException("This method is not implemented yet.");
  }


  @Override
  public void deleteAccount(int accountId) throws AccountNotFoundException {
    accountDAO.deleteAccount(accountId);
  }


  @Override
  public Status updateComponent(String accountId, Credentials credentials, Component component) {
    throw new UnsupportedOperationException("This method is not implemented yet.");
  }

  @Override
  public Account getAccount(int accountId) {
    return accountDAO.getAccount(accountId);
  }

  @Override
  public VPC getVPC(int accountId, int vpcId) {
    return vpcDao.getVPC(accountId, vpcId);
  }

  @Override
  public void deleteVPC(int accountId, int vpcId) throws VPCNotFoundException {
    vpcDao.removeVPC(accountId, vpcId);
  }

  @Override
  public void deleteVPC(String vpcName) throws VPCNotFoundException {
    vpcDao.removeVPC(vpcName);
  }

  @Override
  public Account getAccount(String emailId) {
    return accountDAO.getAccount(emailId);
  }

  @Override
  public List<VPC> getVPC(int accountId) {
    return vpcDao.getVPC(accountId);
  }

  @Override
  public List<VPC> getVPC(String apiKey) {
   return vpcDao.getVPC(apiKey);
  }

  @Override
  public void updateAccount(int accountId, Map<String, Object> params) {
    accountDAO.updateAccount(accountId, params);
  }

  @Override
  public void changePassword(int accountId, String oldPassword, String newPassword) {
    accountDAO.changePassword(accountId, oldPassword, newPassword);
  }

  @Override
  public Account resetPassword(int nonceId, String password) {
    return accountDAO.resetPassword(nonceId, password);
  }

  @Override
  public boolean isValidVPC(String vpcName) {
    return (!profanityFilter.isFiltered(vpcName) && vpcDao.getVPCCount(vpcName) == 0);
  }

  @Override
  public void regenerateApiKey(int accountId) {
    accountDAO.regenerateApiKey(accountId);
  }


  public VPC addVPC(int accountId, VPC vpc) {
    if (!profanityFilter.isFiltered(vpc.getVpcName())) {
       return vpcDao.addVPC(accountId, vpc);
    }
    return null;
  }

  @Override
  public Account getAccountForVPC(String vpcName) {
    return vpcDao.getAccountForVPC(vpcName);
  }


  @Override
  public RolesAccounts getAccountRoles(String vpcName) {
    return vpcDao.getRolesAccounts(vpcName);
  }

  @Override
  public Organization createOrganization(String id, String name) throws OrganizationAlreadyExistsException {
    return organizationDAO.createOrganization(id, name);
  }

  @Override
  public Organization getOrganization(String id) throws OrganizationNotFoundException {
    return organizationDAO.getOrganization(id);
  }

  @Override
  public Organization updateOrganization(String id, String name)  throws OrganizationNotFoundException {
    return organizationDAO.updateOrganization(id, name);
  }

  @Override
  public void deleteOrganization(String id) throws OrganizationNotFoundException {
    organizationDAO.deleteOrganization(id);
  }

  @Override
  public void updateAccountOrganization(int accountId, String orgId)
      throws AccountNotFoundException, OrganizationNotFoundException {
    accountDAO.updateOrganizationId(accountId, orgId);
  }
}
