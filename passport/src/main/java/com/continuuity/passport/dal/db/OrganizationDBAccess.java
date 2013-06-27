package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.AccountAlreadyExistsException;
import com.continuuity.passport.core.exceptions.OrganizationAlreadyExistsException;
import com.continuuity.passport.core.exceptions.OrganizationNotFoundException;
import com.continuuity.passport.dal.OrganizationDAO;
import com.continuuity.passport.meta.Organization;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Implementation of OrganizationDAO with Database as the data store.
 */
public class OrganizationDBAccess extends DBAccess implements OrganizationDAO {

  private final DBConnectionPoolManager poolManager;

  /**
   * Guice injected AccountDBAccess. The parameters needed for DB will be injected as well.
   */
  @Inject
  public OrganizationDBAccess(DBConnectionPoolManager poolManager) {
    this.poolManager = poolManager;
  }

  @Override
  public Organization createOrganization(String id, String name) throws OrganizationAlreadyExistsException {
    Connection connection = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("INSERT INTO %s (%s, %s) VALUES (?,?)",
                                 DBUtils.Organization.TABLE_NAME,
                                 DBUtils.Organization.ID,
                                 DBUtils.Organization.NAME);
      PreparedStatement ps = connection.prepareStatement(sql);
      try {
        ps.setString(1, id);
        ps.setString(2, name);
        ps.execute();
      } finally {
        close(ps);
      }
    } catch (SQLException e) {
      if (DBUtils.DB_INTEGRITY_CONSTRAINT_VIOLATION_DUP_KEY.equals(e.getSQLState())) {
        throw new OrganizationAlreadyExistsException(e.getMessage());
      } else {
        throw Throwables.propagate(e);
      }
    }
    finally {
      close(connection);
    }
    return new Organization(id, name);
  }

  @Override
  public Organization getOrganization(String id) {
    Connection connection = null;
    Organization organization = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("SELECT %s, %s from %s WHERE %s = ?",
                                 DBUtils.Organization.ID,
                                 DBUtils.Organization.NAME,
                                 DBUtils.Organization.TABLE_NAME,
                                 DBUtils.Organization.ID
      );
      PreparedStatement ps = connection.prepareStatement(sql);
      try {
        ps.setString(1, id);
        ResultSet rs = ps.executeQuery();
        try {
          int count = 0;
          while (rs.next()) {
            count++;
            organization = new Organization(rs.getString(DBUtils.Organization.ID),
                                            rs.getString(DBUtils.Organization.NAME));
            if (count > 1) {
              // Note: This condition should never occur since ids have unique constraint. Adding this as a safety net.
              throw new RuntimeException("Multiple organization with same ID");
            }
          }
        } finally {
          close(rs);
        }
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
    finally {
      close(connection);
    }
    return organization;
  }

  @Override
  public Organization updateOrganization(String id, String name) throws OrganizationNotFoundException{
    Connection connection = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("UPDATE %s SET %s = ? WHERE %s = ?",
                                 DBUtils.Organization.TABLE_NAME,
                                 DBUtils.Organization.NAME,
                                 DBUtils.Organization.ID
      );
      PreparedStatement ps = connection.prepareStatement(sql);
      try {
        ps.setString(1, name);
        ps.setString(2, id);
        int affectedRows = ps.executeUpdate();
        if (affectedRows == 0) {
          throw new OrganizationNotFoundException("Organization doesn't exists");
        }
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
    finally {
      close(connection);
    }
    return new Organization(id, name);
  }

  @Override
  public void deleteOrganization(String id) throws OrganizationNotFoundException{
    Connection connection = null;
    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("DELETE FROM %s WHERE %s = ?",
                                 DBUtils.Organization.TABLE_NAME,
                                 DBUtils.Organization.ID
      );
      PreparedStatement ps = connection.prepareStatement(sql);
      try {
        ps.setString(1, id);
        int affectedRows = ps.executeUpdate();
        if (affectedRows == 0) {
          throw new OrganizationNotFoundException("Organization doesn't exists");
        }
      } finally {
        ps.close();
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    }
    finally {
      close(connection);
    }
  }
}
