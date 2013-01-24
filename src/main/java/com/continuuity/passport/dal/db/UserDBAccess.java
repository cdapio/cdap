package com.continuuity.passport.dal.db;

import com.continuuity.passport.core.User;
import com.continuuity.passport.dal.UserDAO;

import java.util.Map;


/**
 *
 */

public class UserDBAccess implements UserDAO {

  private Map<String,String> configuration;
  public UserDBAccess(Map<String, String> configuration) {
    this.configuration = configuration;
  }

  @Override
  public boolean createUser(String id, User user) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean updateUser(String id, User user) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public boolean deleteUser(String id, User user) throws RuntimeException {
    return false;  //To change body of implemented methods use File | Settings | File Templates.
  }
}
