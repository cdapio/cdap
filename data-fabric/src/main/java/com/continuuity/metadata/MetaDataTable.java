package com.continuuity.metadata;

import com.continuuity.data.operation.OperationContext;
import com.continuuity.data2.OperationException;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Interface that defines CRUD opertions on MetaDataTable.
 */
public interface MetaDataTable {

  public static final int DEFAULT_RETRIES_ON_CONFLICT = 3;
  public static final String META_DATA_TABLE_NAME = "meta";

  /**
   * Adds a new entry with conflict resolution.
   * @param context the OperationContext of the caller
   * @param entry the meta data entry to write
   * @throws OperationException with status WRITE_CONFLICT if an entry with
   *    the name and type already exists for the same account and app,
   *    also throws OperationException for other data fabric problems.
   */
  public void add(OperationContext context,
                  MetaDataEntry entry) throws OperationException;

  /**
   * Adds a new entry with the option of conflict resolution.
   * @param context the OperationContext of the caller
   * @param entry the meta data entry to write
   * @param resolve if true, write conflicts are resolved by reading the
   *                latest value of the entry and comparing it with the
   *                entry to be written. If they are the same, then the
   *                write conflict is ignored silently.
   * @throws OperationException with status WRITE_CONFLICT if an entry with
   *    the name and type already exists for the same account and app,
   *    also throws OperationException for other data fabric problems.
   */
  public void add(OperationContext context,
                  MetaDataEntry entry,
                  boolean resolve)
      throws OperationException;

  /**
   * Updates an existing entry with conflict resolution.
   * @param context the OperationContext of the caller
   * @param entry the meta data entry to write
   * @throws OperationException with status ENTRY_NOT_FOUND if an entry with
   *    that name and type does not exist for the given account and app.
   *    also throws OperationException for other data fabric problems.
   */
  public void update(OperationContext context,
                     MetaDataEntry entry) throws OperationException;

  /**
   * Updates an existing entry with the option of conflict resolution.
   * @param context the OperationContext of the caller
   * @param entry the meta data entry to write
   * @param resolve if true, write conflicts are resolved by reading the
   *                latest value of the entry and comparing it with the
   *                entry to be written. If they are the same, then the
   *                write conflict is ignored silently.
   * @throws OperationException with status ENTRY_NOT_FOUND if an entry with
   *    the name and type does not exist for the given account and app.
   *    also throws OperationException for other data fabric problems.
   */
  public void update(OperationContext context,
                     MetaDataEntry entry,
                     boolean resolve) throws OperationException;

  /**
   * Swaps an existing entry with conflict resolution.
   * @param context the OperationContext of the caller
   * @param expected the expected meta data entry before the write
   * @param entry the meta data entry to write
   * @throws OperationException with status ENTRY_NOT_FOUND if an entry with
   *    the name and type does not exist for the given account and app.
   *    also throws OperationException for other data fabric problems.
   */
  public void swap(OperationContext context,
                   MetaDataEntry expected,
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
   * @param newValue The new value for that column
   * @param retryAttempts How many times to retry in case of write conflicts
   * @throws OperationException for data fabric errors. Specifically, if the
   *    entry does not exist yet, status code is ENTRY_NOT_FOUND, and if the
   *    number of retries after write conflict is exhausted, the status code is
   *    WRITE_CONFLICT.
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
   * @param newValue The new value for that column
   * @param retryAttempts How many times to retry in case of write conflicts
   * @throws OperationException for data fabric errors. Specifically, if the
   *    entry does not exist yet, status code is ENTRY_NOT_FOUND, and if the
   *    number of retries after write conflict is exhausted, the status code is
   *    WRITE_CONFLICT.
   */
  public void updateField(OperationContext context,
                          String account, String application,
                          String type, String id,
                          String field, byte[] newValue,
                          int retryAttempts)
      throws OperationException;

  /**
   * Compare-and-swap for a single text field of an entry with concurrency
   * control. Like updateField, with the addition that the write is only
   * performed if the field has the given, expected old value
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @param type The type of entry, must not be null
   * @param id The unique id of the entry (per account, app, type), non-null
   * @param field The name of the field to update, must not be null
   * @param oldValue The new value for that column
   * @param newValue The new value for that column
   * @param retryAttempts How many times to retry in case of write conflicts
   * @throws OperationException for data fabric errors. Specifically, if the
   *    entry does not exist yet, status code is ENTRY_NOT_FOUND, and if the
   *    number of retries after write conflict is exhausted, the status code is
   *    WRITE_CONFLICT.
   */
  public void swapField(OperationContext context,
                        String account, String application,
                        String type, String id,
                        String field, String oldValue, String newValue,
                        int retryAttempts)
      throws OperationException;

  /**
   * Compare-and-swap for a single binary field of an entry with concurrency
   * control. Like updateField, with the addition that the write is only
   * performed if the field has the given, expected old value
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @param type The type of entry, must not be null
   * @param id The unique id of the entry (per account, app, type), non-null
   * @param field The name of the field to update, must not be null
   * @param oldValue The old value for that column
   * @param newValue The new value for that column
   * @param retryAttempts How many times to retry in case of write conflicts
   * @throws OperationException for data fabric errors. Specifically, if the
   *    entry does not exist yet, status code is ENTRY_NOT_FOUND, and if the
   *    number of retries after write conflict is exhausted, the status code is
   *    WRITE_CONFLICT.
   */
  public void swapField(OperationContext context,
                        String account, String application,
                        String type, String id,
                        String field, byte[] oldValue, byte[] newValue,
                        int retryAttempts)
      throws OperationException;

  /**
   * Delete by name & type. This silently succeeds if the entry does not exist.
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @param type The type of entry, must not be null
   * @param id The unique id of the entry (per account, app, type), non-null
   * @throws OperationException for data fabric errors.
   */
  public void delete(OperationContext context,
                     String account, String application,
                     String type, String id)
      throws OperationException;

  /**
   * Delete all the metadata entries specified.
   *
   * @param accountId           AccountId corresponding to the metadata.
   * @param entries             List of entired to be deleted.
   * @throws OperationException for data fabric errors.
   */
  public void delete(String accountId, List<MetaDataEntry> entries)
      throws OperationException;

  /**
   * Get by name & type.
   *
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @param type The type of entry, must not be null
   * @param id The unique id of the entry (per account, app, type), non-null
   * @return the named entry, or null if it does not exist.
   * @throws OperationException for data fabric errors
   */
  public MetaDataEntry get(OperationContext context,
                           String account, String application,
                           String type, String id)
      throws OperationException;

  /**
   * List all entries of a given type.
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @param type The type of entry, must not be null
   * @param fields An optional map of field names and values. If non-null,
   *               then an entry is only included in the result if it has
   *               all fields in the map with the same values.
   * @return the list of matching entries. The list is empty if there are no
   * matching entries.
   * @throws OperationException if something goes wrong
   */
  public List<MetaDataEntry> list(OperationContext context,
                                  String account, String application,
                                  String type, Map<String, String> fields)
      throws OperationException;

  /**
   * List all entries of a given type and id upto a max count.
   *
   * @param context     The operation context of the caller.
   * @param account     The account of the entry, must not be null.
   * @param application The application of the entry, may be null.
   * @param type        The type of entry, must not be null.
   * @param startId     The id to start reading the entries.
   * @param stopId      The id to stop reading the entries.
   * @param count       Max number of entries to be read.
   * @return            The list of metadata entries.
   */
  public List<MetaDataEntry> list(OperationContext context,
                                  String account, String application,
                                  String type, String startId, String stopId, int count)
       throws OperationException;

  /**
   * Delete all entries for an account or application within an account.
   * @param context The operation context of the caller.
   * @param account The account of the entry, must not be null
   * @param application The application of the entry, may be null
   * @throws OperationException if something goes wrong
   */
  public void clear(OperationContext context,
                    String account, String application)
      throws OperationException;

  /**
   * Gets all accounts.
   * @param context The operation context of the caller.
   * @throws OperationException if something goes wrong
   */
  public Collection<String> listAccounts(OperationContext context)
      throws OperationException;

  /**
   * Upgrades the metadata table definition for any configuration or coprocessor changes.
   */
  public void upgrade() throws Exception;
}
