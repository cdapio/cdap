/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */

package com.continuuity.passport.dal.db;

import com.continuuity.common.db.DBConnectionPoolManager;
import com.continuuity.passport.core.exceptions.StaleNonceException;
import com.continuuity.passport.core.utils.NonceUtils;
import com.continuuity.passport.dal.NonceDAO;
import com.google.common.base.Throwables;
import com.google.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 *
 */
public class NonceDBAccess extends DBAccess implements NonceDAO {

  private final DBConnectionPoolManager poolManager;
  private static final int SHORT_EXPIRATION_MILLS = 1000 * 60 * 10;
  private static final int LONG_EXPIRATION_MILLIS = 1000 * 60 * 60 * 24 * 3;


  @Inject
  public NonceDBAccess(DBConnectionPoolManager poolManager) {
    this.poolManager = poolManager;
  }

  /**
   * Generate a random nonce and update in DB.
   * @param id id
   * @param expiration expiration time in seconds
   * @return integer random nonce
   */
  private int updateRandomNonce(String id, int expiration) {
    int nonce = NonceUtils.getNonce();
    try {
      insertOrUpdateNonce(id, expiration, nonce);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return nonce;
  }

  /**
   * Generate a hashed nonce and update in DB.
   * @param id id
   * @param expiration expiration time in seconds
   * @return integer random nonce
   */
  private int updateHashedNonce(String id, int expiration) {
    int nonce = NonceUtils.getNonce(id);
    try {
      insertOrUpdateNonce(id, expiration, nonce);
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return nonce;
  }

  /**
   * Insert or update Nonce in DB.
   * @param id id
   * @param expiration expiration time in seconds
   * @param nonce nonce value to be updated
   */
  private void insertOrUpdateNonce(String id, int expiration, int nonce) {
    Connection connection = this.poolManager.getValidConnection();;
    String sql = String.format("SELECT COUNT(*) FROM %s where %s = ?",
                               DBUtils.Nonce.TABLE_NAME, DBUtils.Nonce.ID_COLUMN);
    try {
      PreparedStatement ps = connection.prepareStatement(sql);
      try {
        ps.setString(1, id);
        ResultSet rs = ps.executeQuery();
        if (rs.next()) {
          int count = rs.getInt(1);
          if (count == 0) {
            insertNonce(id, expiration, nonce);
          } else {
            updateNonce(id, expiration, nonce);
          }
        } else {
          //result set does not have any items for count query.
          throw new RuntimeException("Error in accessing nonce table");
        }
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      } finally {
        close(ps);
      }
    }  catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection);
    }
  }

  /**
   * Insert Nonce in DB.
   * @param id id
   * @param expiration expiration time in seconds
   * @param nonce nonce value to be updated
   * @return integer random nonce
   */
  private void insertNonce(String id, int expiration, int nonce) {

    Connection connection = this.poolManager.getValidConnection();
    String sql = String.format("INSERT INTO %s (%s, %s, %s) VALUES (?,?,?)",
                               DBUtils.Nonce.TABLE_NAME, DBUtils.Nonce.NONCE_ID_COLUMN,
                               DBUtils.Nonce.ID_COLUMN, DBUtils.Nonce.NONCE_EXPIRES_AT_COLUMN);
    try {
      PreparedStatement ps = connection.prepareStatement(sql);
      Timestamp expirationTime = new Timestamp(System.currentTimeMillis() + expiration);
      try {
        ps.setInt(1, nonce);
        ps.setString(2, id);
        ps.setTimestamp(3, expirationTime);
        ps.executeUpdate();
        connection.commit();
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      } finally {
        close(ps);
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection);
    }
  }

  /**
   * Update Nonce in DB.
   * @param id id
   * @param expiration expiration time in seconds
   * @param nonce nonce value to be updated
   * @return integer random nonce
   */
  private void updateNonce(String id, int expiration, int nonce) {

    Connection connection = this.poolManager.getValidConnection();
    String sql = String.format("UPDATE %s SET %s = ?, %s = ? WHERE %s = ?",
                               DBUtils.Nonce.TABLE_NAME, DBUtils.Nonce.NONCE_ID_COLUMN,
                               DBUtils.Nonce.NONCE_EXPIRES_AT_COLUMN, DBUtils.Nonce.ID_COLUMN);
    try {
      PreparedStatement ps = connection.prepareStatement(sql);
      Timestamp expirationTime = new Timestamp(System.currentTimeMillis() + expiration);
      try {
        ps.setInt(1, nonce);
        ps.setString(2, id);
        ps.setTimestamp(3, expirationTime);
        ps.executeUpdate();
        connection.commit();
      } catch (SQLException e) {
        throw Throwables.propagate(e);
      } finally {
        close(ps);
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection);
    }
  }

  @Override
  public int getNonce(String id, NONCE_TYPE nonceType) {

    int nonce = -1;
    try {
      switch (nonceType) {
        case SESSION:
          nonce = updateRandomNonce(id, SHORT_EXPIRATION_MILLS);
          break;
        case ACTIVATION:
          nonce = updateHashedNonce(id, LONG_EXPIRATION_MILLIS);
          break;
        case RESET:
          nonce = updateHashedNonce(id, LONG_EXPIRATION_MILLIS);
          break;
        default:
          throw new RuntimeException("Unknown nonce type");
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
    return nonce;
  }

  private void deleteNonce(int nonce) {

    Connection connection = null;
    PreparedStatement ps = null;

    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("DELETE FROM %s WHERE %s = ?",
        DBUtils.Nonce.TABLE_NAME,
        DBUtils.Nonce.NONCE_ID_COLUMN);
      ps = connection.prepareStatement(sql);
      ps.setInt(1, nonce);
      ps.executeUpdate();
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
    }
  }

  @Override
  public String getId(int nonce, NONCE_TYPE type) throws StaleNonceException {

    Connection connection = null;
    PreparedStatement ps = null;
    String id = null;

    try {
      connection = this.poolManager.getValidConnection();
      String sql = String.format("SELECT %s, %s FROM %s WHERE %s = ?",
        DBUtils.Nonce.ID_COLUMN,
        DBUtils.Nonce.NONCE_EXPIRES_AT_COLUMN,
        DBUtils.Nonce.TABLE_NAME,
        DBUtils.Nonce.NONCE_ID_COLUMN);

      ps = connection.prepareStatement(sql);
      ps.setInt(1, nonce);
      ResultSet rs = ps.executeQuery();

      int count = 0;
      while (rs.next()) {
        id = rs.getString(1);
        Timestamp t = rs.getTimestamp(2);
        if (t.getTime() < System.currentTimeMillis()) {
          throw new StaleNonceException("Older timestamp");
        }
        count++;
        if (count > 1) { // Note: This condition should never occur since ids are auto generated.
          throw new RuntimeException("Multiple nonce with same  ID");
        }
      }
    } catch (SQLException e) {
      throw Throwables.propagate(e);
    } finally {
      close(connection, ps);
      // Delete the nonce after it is used for session
      // For activation and reset the nonce will be deleted by the upstream.
      if (id != null && !id.isEmpty() && NONCE_TYPE.SESSION.equals(type)) {
        deleteNonce(nonce);
      }
      return id;
    }
  }
}

