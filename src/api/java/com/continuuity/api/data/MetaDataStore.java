package com.continuuity.api.data;

import java.util.List;
import java.util.Map;

public interface MetaDataStore {

  public final int DEFAULT_RETRIES_ON_CONFLICT = 3;

  /** adds a new entry, throws if an entry with that name already exists */
  public void add(OperationContext context,
                  MetaDataEntry entry) throws OperationException;

  /** updates an entry, throws if an entry with that name does not exist */
  public void update(OperationContext context,
                     MetaDataEntry entry) throws OperationException;

  /**
   * Updates a single text field of an entry with concurrency control. If
   * the entry is updated by someone else between the read and the write,
   * then this is a write conflict, and the update fails. A specified number
   * of retries is attempted (0 for no retry, -1 for default).
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @param type The type of entry, must not be null
   * @param id The unique id of the entry (per account, app, type), non-null
   * @param field The name of the field to update, must not be null
   * @param newValue The newValue for that column
   * @param retryAttempts How many times to retry in case of write conflicts
   * @throws OperationException if an entry with that name does not exist,
   *    or if the number of retries after write conflict is exhausted
   */
  public void updateField(OperationContext context,
                          String account, String application,
                          String type, String id,
                          String field, String newValue,
                          int retryAttempts)
      throws OperationException;

  /**
   * Updates a single binary field of an entry with concurrency control. If
   * the entry is updated by someone else between the read and the write,
   * then this is a write conflict, and the update fails. A specified number
   * of retries is attempted (0 for no retry, -1 for default).
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @param type The type of entry, must not be null
   * @param id The unique id of the entry (per account, app, type), non-null
   * @param field The name of the field to update, must not be null
   * @param newValue The newValue for that column
   * @param retryAttempts How many times to retry in case of write conflicts
   * @throws OperationException if an entry with that name does not exist,
   *    or if the number of retries after write conflict is exhausted
   */
  public void updateField(OperationContext context,
                          String account, String application,
                          String type, String id,
                          String field, byte[] newValue,
                          int retryAttempts)
      throws OperationException;

  /** delete by name & type */
  public void delete(OperationContext context,
                     String account, String application,
                     String type, String name)
      throws OperationException;

  /** get by name & type */
  public MetaDataEntry get(OperationContext context,
                           String account, String application,
                           String type, String name)
      throws OperationException;

  /** list all entries of a given type */
  public List<MetaDataEntry> list(OperationContext context,
                                  String account, String application,
                                  String type, Map<String, String> fields)
      throws OperationException;

  /** delete all entries for an account or application */
  public void clear(OperationContext context,
                    String account, String application)
      throws OperationException;
}
