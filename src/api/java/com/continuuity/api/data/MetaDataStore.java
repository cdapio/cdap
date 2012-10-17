package com.continuuity.api.data;

import java.util.List;
import java.util.Map;

public interface MetaDataStore {

  public final int DEFAULT_RETRIES_ON_CONFLICT = 3;

  /**
   * adds a new entry.
   * @throws OperationException with status WRITE_CONFLICT if an entry with
   * the name and type already exists for the same account and app,
   * also throws OperationException for other data fabric problems.
   */
  public void add(OperationContext context,
                  MetaDataEntry entry) throws OperationException;

  /** updates an existing entry.
   * @throws OperationException with status ENTRY_NOT_FOUND if an entry with
   * the that name and type does not exist for the given account and app.
   * also throws OperationException for other data fabric problems.
   */
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
   * @throws OperationException for data fabric errors. Specifically, if the
   * entry does not exist yet, status code is ENTRY_NOT_FOUND, and if the
   * number of retries after write conflict is exhausted, the status code is
   * WRITE_CONFLICT.
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
   * @throws OperationException for data fabric errors. Specifically, if the
   * entry does not exist yet, status code is ENTRY_NOT_FOUND, and if the
   * number of retries after write conflict is exhausted, the status code is
   * WRITE_CONFLICT.
   */
  public void updateField(OperationContext context,
                          String account, String application,
                          String type, String id,
                          String field, byte[] newValue,
                          int retryAttempts)
      throws OperationException;

  /**
   * Delete by name & type. This silently succeeds if the entry does not exist.
   * @throws OperationException for data fabric errors.
   */
  public void delete(OperationContext context,
                     String account, String application,
                     String type, String name)
      throws OperationException;

  /**
   * Get by name & type.
   * @return the named entry, or null if it does not exist.
   * @throws OperationException for data fabric errors
   *
   */
  public MetaDataEntry get(OperationContext context,
                           String account, String application,
                           String type, String name)
      throws OperationException;

  /**
   * List all entries of a given type
   * @return the list of matching entries. The list is empty if there are no
   * matching entries.
   * @throws OperationException if something goes wrong
   */
  public List<MetaDataEntry> list(OperationContext context,
                                  String account, String application,
                                  String type, Map<String, String> fields)
      throws OperationException;

  /** delete all entries for an account or application */
  public void clear(OperationContext context,
                    String account, String application)
      throws OperationException;
}
