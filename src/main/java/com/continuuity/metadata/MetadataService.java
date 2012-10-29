package com.continuuity.metadata;

import com.continuuity.api.data.MetaDataEntry;
import com.continuuity.api.data.MetaDataStore;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of thrift meta data service handler.
 */
public class MetadataService extends MetadataHelper
    implements com.continuuity.metadata.thrift.MetadataService.Iface
{
  private static final Logger Log
      = LoggerFactory.getLogger(MetadataService.class);

  private final MetaDataStore mds;

  /**
   * Construction of metadata service handler
   * @param opex instance of opex.
   */
  public MetadataService(OperationExecutor opex) {
    this.mds = new SerializingMetaDataStore(opex);
  }

  /**
   * Creates a stream if not exist.
   * <p>
   *   Stream creation requires id, name and description to be present.
   *   Without these fields a stream creation would fail. If a stream
   *   already exists, then it will be untouched.
   * </p>
   *
   * @param stream information about stream.
   * @return true if successful; false otherwise
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue with creating
   *          stream.
   */
  @Override
  public boolean createStream(Account account, Stream stream)
    throws MetadataServiceException, TException {

    // Validate account and stream
    validateAccount(account);
    validateStream(stream);

    // Create a context.
    OperationContext context = new OperationContext(account.getId());

    try {
      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry = mds.get(
          context, account.getId(), null, FieldTypes.Stream.ID, stream.getId());
      if (readEntry == null) {
        // attempt to add, but in case of write conflict we must read
        // again and try to resolve the conflict
        MetaDataEntry entry = makeEntry(account, stream);
        try {
          // Invoke MDS to add entry
          mds.add(context, entry);
          return true;
        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          // read again for conflict resolution
          readEntry = mds.get(context,
                account.getId(), null, FieldTypes.Stream.ID, stream.getId());
        }
      }

      // loop a few times for write conflict resolution
      for (int attempts = 3; attempts > 0; --attempts) {
        // there is already an entry, determine how it compare to the new one
        CompareStatus status = readEntry == null
            ? CompareStatus.SUPER : compare(stream, readEntry);
        // existing entry is equal or a superset of the new one -> good
        if (status.equals(CompareStatus.EQUAL) ||
            status.equals(CompareStatus.SUB))
          return true;
        else if (status.equals(CompareStatus.DIFF)) {
          // new entry is incompatible with existing -> conflict!
          throw new MetadataServiceException("another, incompatible meta " +
              "data entry already exists.");
        }

        // Create a new metadata entry for update
        MetaDataEntry entry = makeEntry(account, stream);
        try {
          // Invoke MDS to update entry, this can again fail with write conflict
          if (readEntry == null) {
            mds.add(context, entry);
          } else {
            mds.update(context, entry);
          }
          return true;

        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          if (attempts <= 1)
            throw e; // number of attempts exhausted
          // read again for conflict resolution
          readEntry = mds.get(context,
              account.getId(), null, FieldTypes.Stream.ID, stream.getId());
        }
      }
      // unreachable but java does not detect that
      throw new OperationException(StatusCode.INTERNAL_ERROR,
          "this code should be unreachable");

    } catch (OperationException e) {
      Log.warn("Failed creating stream {}. Reason: {}", stream, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Deletes a stream if exists.
   *
   * @param stream to be deleted.
   * @return true if successfull; false otherwise
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue with deleting
   *          stream.
   */
  @Override
  public boolean deleteStream(Account account, Stream stream)
    throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // When creating a stream, you need to have id, name and description
    String id = stream.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Stream id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to delete entry.
      // This will also succeed if the entry does not exist
      mds.delete(context, accountId, null, FieldTypes.Stream.ID, id);
      return true;

    } catch (OperationException e) {
      Log.warn("Failed deleting stream {}. Reason : {}", stream,
          e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Retrieve streams associated with account.
   *
   * @param account for which streams need to be retrieved.
   * @return list of stream associated with account.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          throw when there is issue listing the
   *          streams for an account.
   */
  @Override
  public List<Stream> getStreams(Account account)
    throws MetadataServiceException, TException {
    List<Stream> result = Lists.newArrayList();

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to list streams for an account.
      // NOTE: application is null and fields are null.
      Collection<MetaDataEntry> entries =
        mds.list(context, accountId, null, FieldTypes.Stream.ID, null);

      for(MetaDataEntry entry : entries)
        result.add(makeStream(entry));
      return result;

    } catch (OperationException e) {
      Log.warn("Failed listing streams for account {}. Reason : {}",
               accountId, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Returns a single stream
   *
   * @param account account to which the stream belongs to.
   * @param stream Id of the stream for which more information is requested.
   * @return Stream with additional information like name and description else
   * return the Stream with just the id.
   * @throws MetadataServiceException when there is issue reading in the
   *          information for stream.
   */
  @Override
  public Stream getStream(Account account, Stream stream)
    throws MetadataServiceException, TException {

    // Validate account.
    validateAccount(account);
    String accountId = account.getId();

    String id = stream.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Stream does not have an id.");
    }

    try {
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry =
        mds.get(context, accountId, null,
                FieldTypes.Stream.ID, id);

      if(entry != null) {
        // convert the the meta data entry
        return makeStream(entry);
      } else {
        // return a blank object with exists = false
        stream = new Stream(stream.getId());
        stream.setExists(false);
        return stream;
      }
    } catch (OperationException e) {
      Log.warn("Failed to retrieve stream {}. Reason : {}.",
               stream, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Creates a dataset if not exist.
   *
   * @param dataset to be created.
   * @return true if successfull; false otherwise
   * @throws MetadataServiceException when there is issue with creating
   *          a data set.
   */
  @Override
  public boolean createDataset(Account account, Dataset dataset) throws
    MetadataServiceException, TException {

    // Validate account and dataset
    validateAccount(account);
    validateDataset(dataset);

    // Create a context.
    OperationContext context = new OperationContext(account.getId());

    try {
      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry = mds.get(context,
          account.getId(), null, FieldTypes.Dataset.ID, dataset.getId());
      if (readEntry == null) {
        // attempt to add, but in case of write conflict we must read
        // again and try to resolve the conflict
        MetaDataEntry entry = makeEntry(account, dataset);
        try {
          // Invoke MDS to add entry
          mds.add(context, entry);
          return true;
        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          // read again for conflict resolution
          readEntry = mds.get(context,
              account.getId(), null, FieldTypes.Dataset.ID, dataset.getId());
        }
      }

      // loop a few times for write conflict resolution
      for (int attempts = 3; attempts > 0; --attempts) {
        // there is already an entry, determine how it compare to the new one
        CompareStatus status = readEntry == null
            ? CompareStatus.SUPER : compare(dataset, readEntry);
        // existing entry is equal or a superset of the new one -> good
        if (status.equals(CompareStatus.EQUAL) ||
            status.equals(CompareStatus.SUB))
          return true;
        else if (status.equals(CompareStatus.DIFF)) {
          // new entry is incompatible with existing -> conflict!
          throw new MetadataServiceException("another, incompatible meta " +
              "data entry already exists.");
        }

        // Create a new metadata entry for update
        MetaDataEntry entry = makeEntry(account, dataset);
        try {
          // Invoke MDS to update entry, this can again fail with write conflict
          if (readEntry == null) {
            mds.add(context, entry);
          } else {
            mds.update(context, entry);
          }
          return true;

        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          if (attempts <= 1)
            throw e; // number of attempts exhausted
          // read again for conflict resolution
          readEntry = mds.get(context,
              account.getId(), null, FieldTypes.Dataset.ID, dataset.getId());
        }
      }
      // unreachable but java does not detect that
      throw new OperationException(StatusCode.INTERNAL_ERROR,
          "this code should be unreachable");

    } catch (OperationException e) {
      Log.warn("Failed creating dataset {}. Reason: {}", dataset,
          e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Deletes a dataset.
   *
   * @param dataset to be deleted.
   * @return true if successfull; false otherwise.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          throw when there is issue with creating
   *          a data set.
   */
  @Override
  public boolean deleteDataset(Account account, Dataset dataset) throws
    MetadataServiceException, TException {
    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // When creating a dataset, you need to have id, name and description
    String id = dataset.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Dataset id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to delete entry.
      // This will also succeed if the entry does not exist
      mds.delete(context, accountId, null, FieldTypes.Dataset.ID, id);
      return true;

    } catch (OperationException e) {
      Log.warn("Failed deleting dataset {}. Reason : {}", dataset,
          e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Return all metadata about datasets associated with the account.
   *
   * @param account for which metadata for datasets need to be retrieved.
   * @return list of data set associated with account
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          throw when there is issue with listing
   *          data set.
   */
  @Override
  public List<Dataset> getDatasets(Account account) throws
    MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // Create a context.
    OperationContext context = new OperationContext(accountId);

    try {
      // Invoke MDS to list datasets for an account.
      Collection<MetaDataEntry> entries =
        mds.list(context, accountId, null, FieldTypes.Dataset.ID, null);

      List<Dataset> result = Lists.newArrayList();
      for(MetaDataEntry entry : entries)
        result.add(makeDataset(entry));
      return result;

    } catch (OperationException e) {
      Log.warn("Failed listing datasets for account {}. Reason : {}",
               accountId, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Returns a dataset.
   *
   * @param account to which the dataset belongs to.
   * @param dataset of for which metdata is request.
   * @return Dataset associated with account and dataset.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is an issue with
   *          retrieving the data set.
   */
  @Override
  public Dataset getDataset(Account account, Dataset dataset)
    throws MetadataServiceException, TException {

    // Validate account.
    validateAccount(account);
    String accountId = account.getId();

    String id = dataset.getId();
    if(id == null || id.isEmpty())
      throw new MetadataServiceException("Dataset does not have an id.");

    OperationContext context = new OperationContext(accountId);

    try {
      // Read from meta data store
      MetaDataEntry entry = mds.get(
          context, accountId, null, FieldTypes.Dataset.ID, id);

      if(entry != null) {
        // convert the the meta data entry
        return makeDataset(entry);
      } else {
        // return a blank object with exists = false
        dataset = new Dataset(dataset.getId());
        dataset.setExists(false);
        return dataset;
      }
    } catch (OperationException e) {
      Log.warn("Failed to retrieve dataset {}. Reason : {}.",
               dataset, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Creates an application if not exists.
   *
   * @param account under which the application is created.
   * @param application to be created.
   * @return true if created successfully or already exists, false otherwise.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue with creating
   *          metadata store entry for the application.
   */
  @Override
  public boolean createApplication(Account account, Application application)
    throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    validateApplication(application);

    // Create a context.
    OperationContext context = new OperationContext(account.getId());

    try {
      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry = mds.get(context, account.getId(),
          null, FieldTypes.Application.ID, application.getId());
      if (readEntry == null) {
        // attempt to add, but in case of write conflict we must read
        // again and try to resolve the conflict
        MetaDataEntry entry = makeEntry(account, application);
        try {
          // Invoke MDS to add entry
          mds.add(context, entry);
          return true;
        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          // read again for conflict resolution
          readEntry = mds.get(context, account.getId(),
              null, FieldTypes.Application.ID, application.getId());
        }
      }

      // loop a few times for write conflict resolution
      for (int attempts = 3; attempts > 0; --attempts) {
        // there is already an entry, determine how it compare to the new one
        CompareStatus status = readEntry == null
            ? CompareStatus.SUPER : compare(application, readEntry);
        // existing entry is equal or a superset of the new one -> good
        if (status.equals(CompareStatus.EQUAL) ||
            status.equals(CompareStatus.SUB))
          return true;
        else if (status.equals(CompareStatus.DIFF)) {
          // new entry is incompatible with existing -> conflict!
          throw new MetadataServiceException("another, incompatible meta " +
              "data entry already exists.");
        }

        // Create a new metadata entry for update
        MetaDataEntry entry = makeEntry(account, application);
        try {
          // Invoke MDS to update entry, this can again fail with write conflict
          if (readEntry == null) {
            mds.add(context, entry);
          } else {
            mds.update(context, entry);
          }
          return true;

        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          if (attempts <= 1)
            throw e; // number of attempts exhausted
          // read again for conflict resolution
          readEntry = mds.get(context, account.getId(),
              null, FieldTypes.Application.ID, application.getId());
        }
      }
      // unreachable but java does not detect that
      throw new OperationException(StatusCode.INTERNAL_ERROR,
          "this code should be unreachable");

    } catch (OperationException e) {
      Log.warn("Failed creating application {}. Reason: {}", application,
          e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Deletes an application if exists.
   *
   * @param account the application belongs to.
   * @param application to be deleted.
   * @return true if application was deleted successfully or did not exists to
   *         be deleted; false otherwise.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue deleting an
   *          application.
   */
  @Override
  public boolean deleteApplication(Account account, Application application)
    throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // When creating a application, you need to have id, name and description
    String id = application.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Application id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to delete entry.
      // This will also succeed if the entry does not exist
      mds.delete(context, accountId, null, FieldTypes.Application.ID, id);
      return true;

    } catch (OperationException e) {
      Log.warn("Failed deleting application {}. Reason : {}",
               application, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Returns a list of application associated with account.
   *
   * @param account for which list of applications need to be retrieved.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue listing
   *          applications for a account.
   * @return a list of application associated with account; else empty list.
   */
  @Override
  public List<Application> getApplications(Account account)
    throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to list applications for an account.
      Collection<MetaDataEntry> entries =
        mds.list(context, accountId, null, FieldTypes.Application.ID, null);

      List<Application> result = Lists.newArrayList();
      for(MetaDataEntry entry : entries)
        result.add(makeApplication(entry));
      return result;

    } catch (OperationException e) {
      Log.warn("Failed listing application for account {}. Reason : {}",
               accountId, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Return more information about an application.
   *
   * @param account to the application belongs to.
   * @param application requested for meta data.
   * @return application meta data if exists; else the id passed.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue retrieving
   *          a application from metadata store.
   */
  @Override
  public Application getApplication(Account account, Application application)
    throws MetadataServiceException, TException {

    // Validate account.
    validateAccount(account);
    String accountId = account.getId();

    String id = application.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Application does not have an id.");
    }

    try {
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry = mds.get(
          context, accountId, null, FieldTypes.Application.ID, id);

      if(entry != null) {
        // convert the the meta data entry
        return makeApplication(entry);
      } else {
        // return a blank object with exists = false
        application = new Application(id);
        application.setExists(false);
        return application;
      }

    } catch (OperationException e) {
      Log.warn("Failed to retrieve application {}. Reason : {}.",
               application, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Creates an query if not exists.
   *
   * @param account under which the query is created.
   * @param query to be created.
   * @return true if created successfully or already exists, false otherwise.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue with creating
   *          metadata store entry for the query.
   */
  @Override
  public boolean createQuery(Account account, Query query)
    throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    validateQuery(query);

    // Create a context.
    OperationContext context = new OperationContext(account.getId());

    try {
      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry = mds.get(context, account.getId(),
          null, FieldTypes.Query.ID, query.getId());
      if (readEntry == null) {
        // attempt to add, but in case of write conflict we must read
        // again and try to resolve the conflict
        MetaDataEntry entry = makeEntry(account, query);
        try {
          // Invoke MDS to add entry
          mds.add(context, entry);
          return true;
        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          // read again for conflict resolution
          readEntry = mds.get(context, account.getId(),
              null, FieldTypes.Query.ID, query.getId());
        }
      }

      // loop a few times for write conflict resolution
      for (int attempts = 3; attempts > 0; --attempts) {
        // there is already an entry, determine how it compare to the new one
        CompareStatus status = readEntry == null
            ? CompareStatus.SUPER : compare(query, readEntry);
        // existing entry is equal or a superset of the new one -> good
        if (status.equals(CompareStatus.EQUAL) ||
            status.equals(CompareStatus.SUB))
          return true;
        else if (status.equals(CompareStatus.DIFF)) {
          // new entry is incompatible with existing -> conflict!
          throw new MetadataServiceException("another, incompatible meta " +
              "data entry already exists.");
        }

        // Create a new metadata entry for update
        MetaDataEntry entry = makeEntry(account, query);
        try {
          // Invoke MDS to update entry, this can again fail with write conflict
          if (readEntry == null) {
            mds.add(context, entry);
          } else {
            mds.update(context, entry);
          }
          return true;

        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          if (attempts <= 1)
            throw e; // number of attempts exhausted
          // read again for conflict resolution
          readEntry = mds.get(context, account.getId(),
              null, FieldTypes.Query.ID, query.getId());
        }
      }
      // unreachable but java does not detect that
      throw new OperationException(StatusCode.INTERNAL_ERROR,
          "this code should be unreachable");

    } catch (OperationException e) {
      Log.warn("Failed creating query {}. Reason: {}", query,
          e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  @Override
  public boolean updateQuery(Account account, Query query)
      throws MetadataServiceException, TException {

    // Validate account and query
    validateAccount(account);
    validateQuery(query);

    // create a context
    OperationContext opContext =
        new OperationContext(account.getId(), query.getApplication());

    // perform the update
    try {
      this.mds.update(opContext, makeEntry(account, query));
      return true;
    } catch (OperationException e) {
      if (e.getStatus() == StatusCode.ENTRY_NOT_FOUND) // entry does not exist
        return false;
      Log.warn("Failed to update query {}. Reason: {}.", query, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  @Override
  public boolean addDatasetToQuery(String account, String app,
                                   String query, String dataset)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);

    // create a context
    OperationContext opContext = new OperationContext(account, app);

    // try three times, give up after 3 write conflicts
    for (int i = 0; i < 3; i++) {
      // retrieve the meta data entry
      MetaDataEntry entry;
      try {
        entry = this.mds.get(
            opContext, account, app, FieldTypes.Query.ID, query);
      } catch (OperationException e) {
        Log.warn("Failed to get query for account {}. Reason: {}.",
            account, e.getMessage());
        throw new MetadataServiceException(e.getMessage());
      }
      if (entry == null) throw new
          MetadataServiceException("No meta data found for query " + query);

      String oldValue = entry.getTextField(FieldTypes.Query.DATASETS);
      String newValue =
          oldValue == null ? dataset + " " : oldValue + dataset + " ";

      try {
        mds.swapField(opContext, account, app, FieldTypes.Query.ID, query,
            FieldTypes.Query.DATASETS, oldValue, newValue, -1);
        return true;
      } catch (OperationException e) {
        if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
          Log.warn("Failed to swap field for query {}. Reason: {}.", query,
              e.getMessage());
          throw new MetadataServiceException(e.getMessage());
        }
      }
    }
    // must be a write conflict, repeatedly
    return false;
  }

  /**
   * Deletes a query if exists.
   *
   * @param account the query belongs to.
   * @param query to be deleted.
   * @return true if query was deleted successfully or did not exists to
   *         be deleted; false otherwise.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue deleting an
   *          query.
   */
  @Override
  public boolean deleteQuery(Account account, Query query)
    throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // When deleting a query, you need to have id, name and description
    String id = query.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Application id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to delete entry.
      // This will also succeed if the entry does not exist
      mds.delete(context, accountId, query.getApplication(),
          FieldTypes.Query.ID, id);
      return true;

    } catch (OperationException e) {
      Log.warn("Failed deleting query {}. Reason : {}", query, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  /**
   * Returns a list of query associated with account.
   *
   * @param account for which list of queries need to be retrieved.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue listing
   *          queries for a account.
   * @return a list of queries associated with account; else empty list.
   */
  @Override
  public List<Query> getQueries(Account account)
    throws MetadataServiceException, TException {
    List<Query> result = Lists.newArrayList();

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // Create a context.
    OperationContext context = new OperationContext(accountId);

    // query the meta data store
    Collection<MetaDataEntry> entries;
    try {
      entries = mds.list(context, accountId, null, FieldTypes.Query.ID, null);
    } catch (OperationException e) {
      Log.warn("Failed to list queries for account {}. Reason: {}.",
          account, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }

    for(MetaDataEntry entry : entries) {
        result.add(makeQuery(entry));
    }
    return result;
  }

  /**
   * Return more information about an query.
   *
   * @param account to the query belongs to.
   * @param query requested for meta data.
   * @return query meta data if exists; else the id passed.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue retrieving
   *          a queries from metadata store.
   */
  @Override
  public Query getQuery(Account account, Query query)
    throws MetadataServiceException, TException {

    // Validate account.
    validateAccount(account);
    String accountId = account.getId();

    String id = query.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Query does not have an id.");
    }
    String app = query.getApplication();
    if(app == null || app.isEmpty()) {
      throw new MetadataServiceException("Query does not have an app.");
    }

    try {
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry =
        mds.get(context, accountId, app, FieldTypes.Query.ID, id);

      if(entry != null) {
        // convert the the meta data entry
        return makeQuery(entry);
      } else {
        // return a blank object with exists = false
        query = new Query(query.getId(), query.getApplication());
        query.setExists(false);
        return query;
      }

    } catch (OperationException e) {
      Log.warn("Failed to retrieve query {}. Reason : {}.",
               query, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  @Override
  public boolean createFlow(String accountId, Flow flow) throws
      MetadataServiceException, TException {

    // Validate all account.
    Account account = new Account(accountId);
    validateAccount(account);
    validateFlow(flow);

    // Create a context.
    OperationContext context = new OperationContext(account.getId());

    try {
      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry = mds.get(context, account.getId(),
          null, FieldTypes.Flow.ID, flow.getId());
      if (readEntry == null) {
        // attempt to add, but in case of write conflict we must read
        // again and try to resolve the conflict
        MetaDataEntry entry = makeEntry(account, flow);
        try {
          // Invoke MDS to add entry
          mds.add(context, entry);
          return true;
        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          // read again for conflict resolution
          readEntry = mds.get(context, account.getId(),
              null, FieldTypes.Flow.ID, flow.getId());
        }
      }

      // loop a few times for write conflict resolution
      for (int attempts = 3; attempts > 0; --attempts) {
        // there is already an entry, determine how it compare to the new one
        CompareStatus status = readEntry == null
            ? CompareStatus.SUPER : compare(flow, readEntry);
        // existing entry is equal or a superset of the new one -> good
        if (status.equals(CompareStatus.EQUAL) ||
            status.equals(CompareStatus.SUB))
          return true;
        else if (status.equals(CompareStatus.DIFF)) {
          // new entry is incompatible with existing -> conflict!
          throw new MetadataServiceException("another, incompatible meta " +
              "data entry already exists.");
        }

        // Create a new metadata entry for update
        MetaDataEntry entry = makeEntry(account, flow);
        try {
          // Invoke MDS to update entry, this can again fail with write conflict
          if (readEntry == null) {
            mds.add(context, entry);
          } else {
            mds.update(context, entry);
          }
          return true;

        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT)
            throw e; // we can only handle write conflicts here
          if (attempts <= 1)
            throw e; // number of attempts exhausted
          // read again for conflict resolution
          readEntry = mds.get(context, account.getId(),
              null, FieldTypes.Flow.ID, flow.getId());
        }
      }
      // unreachable but java does not detect that
      throw new OperationException(StatusCode.INTERNAL_ERROR,
          "this code should be unreachable");

    } catch (OperationException e) {
      Log.warn("Failed creating flow {}. Reason: {}", flow,
          e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  @Override
  public boolean updateFlow(String accountId, Flow flow)
      throws MetadataServiceException, TException {

    // Validate all account.
    Account account = new Account(accountId);
    validateAccount(account);
    // validate flow
    validateFlow(flow);

    // create a context
    OperationContext opContext =
        new OperationContext(account.getId(), flow.getApplication());

    // perform the update
    try {
      this.mds.update(opContext, makeEntry(account, flow));
      return true;
    } catch (OperationException e) {
      if (e.getStatus() == StatusCode.ENTRY_NOT_FOUND) // entry does not exist
        return false;
      Log.warn("Failed to update flow {}. Reason: {}.", flow, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  @Override
  public boolean addDatasetToFlow(String account, String app,
                                  String flowid, String dataset)
      throws MetadataServiceException, TException {
    return
        addThingToFlow(account, app, flowid, FieldTypes.Flow.DATASETS, dataset);
  }

  @Override
  public boolean addStreamToFlow(String account, String app,
                                 String flowid, String stream)
      throws MetadataServiceException, TException {
    return
        addThingToFlow(account, app, flowid, FieldTypes.Flow.STREAMS, stream);
  }

  private boolean addThingToFlow(String account, String app,
        String flowid, String thingField, String thing)
    throws MetadataServiceException, TException {

      // Validate all account.
    validateAccount(account);

    // create a context
    OperationContext opContext = new OperationContext(account, app);

    // try three times, give up after 3 write conflicts
    for (int i = 0; i < 3; i++) {
      // retrieve the meta data entry
      MetaDataEntry entry;
      try {
        entry = this.mds.get(
            opContext, account, app, FieldTypes.Flow.ID, flowid);
      } catch (OperationException e) {
        Log.warn("Failed to list flows for account {}. Reason: {}.",
            account, e.getMessage());
        throw new MetadataServiceException(e.getMessage());
      }
      if (entry == null) throw new
          MetadataServiceException("No meta data found for flow " + flowid);

      String oldValue = entry.getTextField(thingField);
      String newValue =
          oldValue == null ? thing + " " : oldValue + thing + " ";

      try {
        mds.swapField(opContext, account, app, FieldTypes.Flow.ID, flowid,
            thingField, oldValue, newValue, -1);
        return true;
      } catch (OperationException e) {
        if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
          Log.warn("Failed to swap field for flow {}. Reason: {}.", flowid,
              e.getMessage());
          throw new MetadataServiceException(e.getMessage());
        }
      }
    }
    // must be a write conflict, repeatedly
    return false;
  }

  @Override
  public boolean deleteFlow(String account, String application, String flowid)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);

    // create a context
    OperationContext opContext =
        new OperationContext(account, application);

    // perform the delete
    try {
      this.mds.delete(opContext,
          account, application, FieldTypes.Flow.ID, flowid);
    } catch (OperationException e) {
      Log.warn("Failed to delete flow {}. Reason: {}.", flowid, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
  }

  @Override
  public List<Flow> getFlows(String account) throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    // create a context
    OperationContext opContext = new OperationContext(account, null);

    // retrieve list of meta entries
    List<MetaDataEntry> entries;
    try {
      entries = this.mds.list(
          opContext, account, null, FieldTypes.Flow.ID, null);
    } catch (OperationException e) {
      Log.warn("Failed to list flows for account {}. Reason: {}.",
          account, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    if (entries == null || entries.isEmpty())
      return Collections.emptyList();

    // convert each meta entry into a Flow
    List<Flow> flows = Lists.newArrayList();
    for (MetaDataEntry entry : entries)
      flows.add(makeFlow(entry));
    return flows;
  }

  @Override
  public Flow getFlow(String account, String application, String flowid)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);

    // create a context
    OperationContext opContext = new OperationContext(account, null);

    // retrieve the meta data entry
    MetaDataEntry entry;
    try {
      entry = this.mds.get(
          opContext, account, application, FieldTypes.Flow.ID, flowid);
    } catch (OperationException e) {
      Log.warn("Failed to get flow for account {}. Reason: {}.",
          account, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    if (entry == null) {
      List<String> emptyList = Collections.emptyList();
      Flow flow = new Flow(flowid, application, "", emptyList, emptyList);
      flow.setExists(false);
      return flow;
    } else
      return makeFlow(entry);
  }

  @Override
  public List<Flow> getFlowsByApplication(String account, String application)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    // create a context
    OperationContext opContext = new OperationContext(account, null);

    // retrieve list of meta entries
    List<MetaDataEntry> entries;
    try {
      entries = this.mds.list(
          opContext, account, application, FieldTypes.Flow.ID, null);
    } catch (OperationException e) {
      Log.warn("Failed to list flows for account {}. Reason: {}.",
          account, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    if (entries == null || entries.isEmpty())
      return Collections.emptyList();

    // convert each meta entry into a Flow
    List<Flow> flows = Lists.newArrayList();
    for (MetaDataEntry entry : entries)
      flows.add(makeFlow(entry));
    return flows;
  }

  @Override
  public List<Query> getQueriesByApplication(String account,
                                            String application)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    // create a context
    OperationContext opContext = new OperationContext(account, null);

    // retrieve list of meta entries
    List<MetaDataEntry> entries;
    try {
      entries = this.mds.list(
          opContext, account, application, FieldTypes.Query.ID, null);
    } catch (OperationException e) {
      Log.warn("Failed to list flows for account {}. Reason: {}.",
          account, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    if (entries == null || entries.isEmpty())
      return Collections.emptyList();

    // convert each meta entry into a Flow
    List<Query> queries = Lists.newArrayList();
    for (MetaDataEntry entry : entries)
      queries.add(makeQuery(entry));
    return queries;
  }

  @Override
  public List<Stream> getStreamsByApplication(String account, String app)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);

    // first get all flows for the app
    List<Flow> flows = getFlowsByApplication(account, app);

    // this will hold all the streams we find in flows
    Map<String, Stream> foundStreams = Maps.newHashMap();

    // now iterate over all flows and get each stream
    for (Flow flow : flows) {
      List<String> flowStreams = flow.getStreams();
      if (flowStreams == null || flowStreams.isEmpty())
        continue;
      for (String streamName : flowStreams) {
        if (foundStreams.containsKey(streamName))
          continue;
        Stream stream =
            getStream(new Account(account), new Stream(streamName));
        if (stream.isExists()) {
          foundStreams.put(streamName, stream);
        }
      }
    }

    // extract the found streams into a list
    List<Stream> streams = Lists.newArrayList();
    for (Stream stream : foundStreams.values())
      streams.add(stream);
    return streams;
  }

  @Override
  public List<Dataset> getDatasetsByApplication(String account, String app)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);

    // this will hold all the datasets we find in flows
    Map<String, Dataset> foundDatasets = Maps.newHashMap();

    // first get all flows for the app
    List<Flow> flows = getFlowsByApplication(account, app);

    // now iterate over all flows and get each dataset
    for (Flow flow : flows) {
      List<String> flowDatasets = flow.getDatasets();
      if (flowDatasets == null || flowDatasets.isEmpty())
        continue;
      for (String datasetName : flowDatasets) {
        if (foundDatasets.containsKey(datasetName))
          continue;
        Dataset dataset =
            getDataset(new Account(account), new Dataset(datasetName));
        if (dataset.isExists()) {
          foundDatasets.put(datasetName, dataset);
        }
      }
    }

    // first get all queries for the app
    List<Query> queries = getQueriesByApplication(account, app);

    // now iterate over all flows and get each dataset
    for (Query query : queries) {
      List<String> queryDatasets = query.getDatasets();
      if (queryDatasets == null || queryDatasets.isEmpty())
        continue;
      for (String datasetName : queryDatasets) {
        if (foundDatasets.containsKey(datasetName))
          continue;
        Dataset dataset =
            getDataset(new Account(account), new Dataset(datasetName));
        if (dataset.isExists()) {
          foundDatasets.put(datasetName, dataset);
        }
      }
    }

    // extract the found datasets into a list
    List<Dataset> datasets = Lists.newArrayList();
    for (Dataset dataset : foundDatasets.values())
      datasets.add(dataset);
    return datasets;
  }

  @Override
  public List<Flow> getFlowsByStream(String account, String stream)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);

    // first get all flows for the app
    List<Flow> flows = getFlows(account);

    // select the flows that read from the stream
    List<Flow> flowsForStream = Lists.newLinkedList();
    for (Flow flow : flows) {
      if (flow.getStreams().contains(stream))
        flowsForStream.add(flow);
    }
    return flowsForStream;
  }

  @Override
  public List<Flow> getFlowsByDataset(String account, String dataset)
      throws MetadataServiceException, TException {
    // Validate all account.
    validateAccount(account);

    // first get all flows for the app
    List<Flow> flows = getFlows(account);

    // select the flows that use the dataset
    List<Flow> flowsForDS = Lists.newLinkedList();
    for (Flow flow : flows) {
      if (flow.getDatasets().contains(dataset))
        flowsForDS.add(flow);
    }
    return flowsForDS;
  }

  @Override
  public List<Query> getQueriesByDataset(String account, String dataset)
      throws MetadataServiceException, TException {
    // Validate all account.
    validateAccount(account);

    // first get all flows for the app
    List<Query> queries = getQueries(new Account(account));

    // select the flows that use the dataset
    List<Query> queriesForDS = Lists.newLinkedList();
    for (Query query : queries) {
      if (query.getDatasets().contains(dataset))
        queriesForDS.add(query);
    }
    return queriesForDS;
  }

  /**
   * Validates the account passed.
   *
   * @param account to be validated.
   * @throws MetadataServiceException thrown if account is null or empty.
   */
  void validateAccount(Account account)
    throws MetadataServiceException {
    // Validate all fields.
    if(account == null) {
      throw new
        MetadataServiceException("Account cannot be null");
    }
    validateAccount(account.getId());
  }
  void validateAccount(String accountId)
      throws MetadataServiceException {
    if(accountId == null || accountId.isEmpty()) {
      throw new
        MetadataServiceException("Account Id cannot be null or empty");
    }
  }
}
