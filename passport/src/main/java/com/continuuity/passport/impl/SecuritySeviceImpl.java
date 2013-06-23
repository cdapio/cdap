package com.continuuity.passport.impl;

import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.service.SecurityService;
import com.continuuity.passport.dal.NonceDAO;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

/**
 * Implementation of SecurityService.
 */
public class SecuritySeviceImpl implements SecurityService {

  private final NonceDAO nonceDAO;

  @Inject
  public SecuritySeviceImpl(NonceDAO nonceDAO) {
    this.nonceDAO = nonceDAO;
  }

  @Override
  public int getActivationNonce(String id) {
    return nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.ACTIVATION);
  }

  @Override
  public int getSessionNonce(String id) {
    return nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.SESSION);
  }

  @Override
  public String getActivationId(int nonce) {
    try {
      return nonceDAO.getId(nonce, NonceDAO.NONCE_TYPE.ACTIVATION);
    } catch (StaleNonceException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public String getSessionId(int nonce) {

    try {
      return nonceDAO.getId(nonce, NonceDAO.NONCE_TYPE.SESSION);
    } catch (StaleNonceException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public int getResetNonce(String id) {
    return nonceDAO.getNonce(id, NonceDAO.NONCE_TYPE.RESET);
  }
}
