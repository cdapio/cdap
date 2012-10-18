package com.continuuity.metadata;

import com.continuuity.api.data.MetaDataEntry;
import com.continuuity.api.data.MetaDataStore;
import com.continuuity.api.data.OperationContext;
import com.continuuity.api.data.OperationException;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.metadata.thrift.*;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Implementation of thrift meta data service handler.
 */
public class MetadataService implements
    com.continuuity.metadata.thrift.MetadataService.Iface {

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

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // When creating a stream, you need to have id, name and description
    String id = stream.getId();
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Stream id is empty or null.");
    }

    if(! stream.isSetName()) {
      throw new MetadataServiceException("Stream name should be set for create");
    }
    String name = stream.getName();
    if(name == null || (name != null && name.isEmpty())) {
      throw new MetadataServiceException("Stream name cannot be null or empty");
    }

    String description = "";
    if(stream.isSetDescription()) {
      description = stream.getDescription();
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry =
        mds.get(context, accountId, null,
                FieldTypes.Stream.ID, id);
      if(readEntry != null) {
        return true;
      }

      // Create a new metadata entry.
      MetaDataEntry entry = new MetaDataEntry(
        accountId, null, FieldTypes.Stream.ID, id
      );

      // Adding other fields.
      entry.addField(FieldTypes.Stream.NAME, name);
      entry.addField(FieldTypes.Stream.DESCRIPTION, description);
      entry.addField(FieldTypes.Stream.CREATE_DATE,
                     String.format("%d", System.currentTimeMillis()));
      if(stream.isSetCapacityInBytes()) {
        entry.addField(FieldTypes.Stream.CAPACITY_IN_BYTES,
                       String.format("%d", stream.getCapacityInBytes()));
      }
      if(stream.isSetExpiryInSeconds()) {
        entry.addField(FieldTypes.Stream.EXPIRY_IN_SECONDS,
                       String.format("%d", stream.getExpiryInSeconds()));
      }

      // Invoke MDS to add entry.
      mds.add(context, entry);
    } catch (OperationException e) {
      Log.warn("Failed creating stream {}. Reason : {}", stream, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
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
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Stream id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry =
        mds.get(context, accountId, null,
                FieldTypes.Stream.ID, id);

      // If stream does not exist, then no point in deleting it.
      if(readEntry == null) {
        return true;
      }

      // Invoke MDS to delete entry.
      mds.delete(context, accountId, null, FieldTypes.Stream.ID, id);
    } catch (OperationException e) {
      Log.warn("Failed deleting stream {}. Reason : {}", stream, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
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
      Collection<MetaDataEntry> streams =
        mds.list(context, accountId, null, FieldTypes.Stream.ID, null);
      for(MetaDataEntry stream : streams) {
        Stream rstream = new Stream(stream.getId());
        rstream.setName(stream.getTextField(FieldTypes.Stream.NAME));
        rstream.setDescription(
          stream.getTextField(FieldTypes.Stream.DESCRIPTION)
        );

        // More fields can be added later when we need them for now
        // we just return id, name & description.
        result.add(rstream);
      }
    } catch (OperationException e) {
      Log.warn("Failed listing streams for account {}. Reason : {}",
               accountId, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return result;
  }

  /**
   * Retruns a single stream with more information.
   *
   * @param account account to which the stream belongs to.
   * @param stream Id of the stream for which more information is requested.
   * @return Stream with additional information like name and description else
   * return the Stream with just the id.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue reading in the
   *          information for stream.
   */
  @Override
  public Stream getStream(Account account, Stream stream)
    throws MetadataServiceException, TException {

    // Validate account.
    validateAccount(account);
    String accountId = account.getId();

    String id = stream.getId();
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Stream does not have an id.");
    }

    try {
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry =
        mds.get(context, accountId, null,
                FieldTypes.Stream.ID, id);

      // Add description and name to stream and return.
      if(entry != null) {
        stream.setName(entry.getTextField(
          FieldTypes.Stream.NAME
        ));
        stream.setDescription(entry.getTextField(
          FieldTypes.Stream.DESCRIPTION
        ));
      } else {
        stream.setExists(false);
      }
    } catch (OperationException e) {
      Log.warn("Failed to retrieve stream {}. Reason : {}.",
               stream, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return stream;
  }

  /**
   * Creates a dataset if not exist.
   *
   * @param dataset to be created.
   * @return true if successfull; false otherwise
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          throw when there is issue with creating
   *          a data set.
   */
  @Override
  public boolean createDataset(Account account, Dataset dataset) throws
    MetadataServiceException, TException {

    // Validate account.
    validateAccount(account);
    String accountId = account.getId();

    // When creating a stream, you need to have id, name and description
    String id = dataset.getId();
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Dataset id is empty or null.");
    }

    if(! dataset.isSetName()) {
      throw new MetadataServiceException("Dataset name should be set for create");
    }

    String name = dataset.getName();
    if(name == null || (name != null && name.isEmpty())) {
      throw new MetadataServiceException("Dataset name cannot be null or empty");
    }

    String description = "";
    if(dataset.isSetDescription()) {
      description = dataset.getDescription();
    }

    if(! dataset.isSetType()) {
      throw new MetadataServiceException("Dataset type should be set for create");
    }

    String type = dataset.getType();

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry =
        mds.get(context, accountId, null,
                FieldTypes.Dataset.ID, id);
      if(readEntry != null) {
        return true;
      }

      // Create a new metadata entry.
      MetaDataEntry entry = new MetaDataEntry(
        accountId, null, FieldTypes.Dataset.ID, id
      );

      // Adding other fields.
      entry.addField(FieldTypes.Dataset.NAME, name);
      entry.addField(FieldTypes.Dataset.DESCRIPTION, description);
      entry.addField(FieldTypes.Dataset.CREATE_DATE,
                     String.format("%d", System.currentTimeMillis()));
      entry.addField(FieldTypes.Dataset.TYPE, type);
      // Invoke MDS to add entry.
      mds.add(context, entry);
    } catch (OperationException e) {
      Log.warn("Failed creating dataset {}. Reason : {}", dataset,
               e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
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

    // When creating a stream, you need to have id, name and description
    String id = dataset.getId();
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Dataset id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry =
        mds.get(context, accountId, null,
                FieldTypes.Dataset.ID, id);

      // If stream does not exist, then no point in deleting it.
      if(readEntry == null) {
        return true;
      }
      // Invoke MDS to delete entry.
      mds.delete(context, accountId, null, FieldTypes.Dataset.ID, id);
    } catch (OperationException e) {
      Log.warn("Failed deleting dataset {}. Reason : {}", dataset, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
  }

  /**
   * Return all metdata about datasets associated with the account.
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
    List<Dataset> result = Lists.newArrayList();

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to list streams for an account.
      // NOTE: application is null and fields are null.
      Collection<MetaDataEntry> datasets =
        mds.list(context, accountId, null, FieldTypes.Dataset.ID, null);
      for(MetaDataEntry dataset : datasets) {
        Dataset rDataset = new Dataset(dataset.getId());
        rDataset.setName(dataset.getTextField(FieldTypes.Dataset.NAME));
        rDataset.setDescription(
          dataset.getTextField(FieldTypes.Dataset.DESCRIPTION)
        );
        try {
          String type = dataset.getTextField(FieldTypes.Dataset.TYPE);
          rDataset.setType(type);
        } catch (NumberFormatException e) {
          Log.warn("Dataset {} has type that is not an integer",
                   rDataset.getName());
        }

        // More fields can be added later when we need them for now
        // we just return id, name & description.
        result.add(rDataset);
      }
    } catch (OperationException e) {
      Log.warn("Failed listing streams for account {}. Reason : {}",
               accountId, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return result;
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
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Dataset does not have an id.");
    }

    try {
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry =
        mds.get(context, accountId, null,
                FieldTypes.Dataset.ID, id);

      // Add description and name to stream and return.
      if(entry != null) {
        dataset.setName(entry.getTextField(
          FieldTypes.Dataset.NAME
        ));
        dataset.setDescription(entry.getTextField(
            FieldTypes.Dataset.DESCRIPTION
        ));
        dataset.setType(entry.getTextField(
            FieldTypes.Dataset.TYPE
        ));
      } else {
        dataset.setExists(false);
      }
    } catch (OperationException e) {
      Log.warn("Failed to retrieve dataset {}. Reason : {}.",
               dataset, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return dataset;
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
    String accountId = account.getId();

    // When creating a stream, you need to have id, name and description
    String id = application.getId();
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Application id is empty or null.");
    }

    String description = "";
    if(application.isSetDescription()) {
      description = application.getDescription();
    }

    if(! application.isSetName()) {
      throw new MetadataServiceException("Application name should be set for create");
    }
    String name = application.getName();
    if(name == null || (name != null && name.isEmpty())) {
      throw new MetadataServiceException("Application name cannot be null or empty");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry =
        mds.get(context, accountId, null,
                FieldTypes.Application.ID, id);
      if(readEntry != null) {
        return true;
      }

      // Create a new metadata entry.
      MetaDataEntry entry = new MetaDataEntry(
        accountId, null, FieldTypes.Application.ID, id
      );

      // Adding other fields.
      entry.addField(FieldTypes.Application.NAME, name);
      entry.addField(FieldTypes.Application.DESCRIPTION, description);
      entry.addField(FieldTypes.Application.CREATE_DATE,
                     String.format("%d", System.currentTimeMillis()));
      // Invoke MDS to add entry.
      mds.add(context, entry);
    } catch (OperationException e) {
      Log.warn("Failed creating application {}. Reason : {}",
               application, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
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

    // When creating a stream, you need to have id, name and description
    String id = application.getId();
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Application id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry =
        mds.get(context, accountId, null,
                FieldTypes.Application.ID, id);

      // If stream does not exist, then no point in deleting it.
      if(readEntry == null) {
        return true;
      }

      // Invoke MDS to delete entry.
      mds.delete(context, accountId, null, FieldTypes.Application.ID, id);
    } catch (OperationException e) {
      Log.warn("Failed deleting application {}. Reason : {}",
               application, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
  }

  /**
   * Returns a list of application associated with account.
   *
   * @param account for which list of applications need to be retrieved.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue listing
   *          applications for a account.
   * @returns a list of application associated with account; else empty list.
   */
  @Override
  public List<Application> getApplications(Account account)
    throws MetadataServiceException, TException {
    List<Application> result = Lists.newArrayList();

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to list streams for an account.
      // NOTE: application is null and fields are null.
      Collection<MetaDataEntry> applications =
        mds.list(context, accountId, null, FieldTypes.Application.ID, null);
      for(MetaDataEntry application : applications) {
        Application rApplication = new Application(application.getId());
        rApplication.setName(application.getTextField(FieldTypes.Application.NAME));
        rApplication.setDescription(
          application.getTextField(FieldTypes.Application.DESCRIPTION)
        );
        // More fields can be added later when we need them for now
        // we just return id, name & description.
        result.add(rApplication);
      }
    } catch (OperationException e) {
      Log.warn("Failed listing application for account {}. Reason : {}",
               accountId, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return result;
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
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Application does not have an id.");
    }

    try {
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry =
        mds.get(context, accountId, null,
                FieldTypes.Application.ID, id);

      // Add description and name to stream and return.
      if(entry != null) {
        application.setName(entry.getTextField(
          FieldTypes.Application.NAME
        ));
        application.setDescription(entry.getTextField(
          FieldTypes.Application.DESCRIPTION
        ));
      }
    } catch (OperationException e) {
      Log.warn("Failed to retrieve application {}. Reason : {}.",
               application, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return application;
  }

  // When creating a query, you need to have id, name and app
  void validateQuery(Query query) throws MetadataServiceException {
    String id = query.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Query id is empty or null.");
    }
    String name = query.getName();
    if(name == null || name.isEmpty()) {
      throw new MetadataServiceException("Query name is empty or null.");
    }
    String app = query.getApplication();
    if(app == null || app.isEmpty()) {
      throw new MetadataServiceException("Query's app name is empty or null.");
    }
    String serviceName = query.getServiceName();
    if(name == null || (name != null && name.isEmpty())) {
      throw new MetadataServiceException("Query service name cannot be null " +
          "or empty");
    }
  }

  private MetaDataEntry makeEntry(String account, Query query) {
    // Create a new metadata entry.
    MetaDataEntry entry = new MetaDataEntry(
        account, query.getApplication(), FieldTypes.Query.ID, query.getId());
     // Adding other fields.
    entry.addField(FieldTypes.Query.NAME, query.getName());
    entry.addField(FieldTypes.Query.DESCRIPTION, query.getDescription());
    entry.addField(FieldTypes.Query.SERVICE_NAME, query.getServiceName());
    entry.addField(FieldTypes.Query.DATASETS, ListToString(query.getDatasets()));
    return entry;
  }

  private Query makeQuery(MetaDataEntry entry) {
    Query query = new Query(entry.getId(), entry.getApplication());
    String name = entry.getTextField(FieldTypes.Query.NAME);
    if (name != null) query.setName(name);
    String description = entry.getTextField(FieldTypes.Query.DESCRIPTION);
    if (description != null) query.setDescription(description);
    String service = entry.getTextField(FieldTypes.Query.SERVICE_NAME);
    if (service != null) query.setServiceName(service);
    String datasets = entry.getTextField(FieldTypes.Query.DATASETS);
    if (datasets != null) query.setDatasets(StringToList(datasets));
    return query;
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

    // Validate account and query
    validateAccount(account);
    validateQuery(query);

    // create a context
    OperationContext opContext =
        new OperationContext(account.getId(), query.getApplication());

    // perform the insert
    try {
      this.mds.add(opContext, makeEntry(account.getId(), query));
      return true;
    } catch (OperationException e) {
      if (e.getStatus() == StatusCode.WRITE_CONFLICT) // entry already exists
        return false;
      Log.warn("Failed to create query {}. Reason: {}.", query, e.getMessage());
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
      this.mds.update(opContext, makeEntry(account.getId(), query));
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
   * Deletes an query if exists.
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

    // When creating a stream, you need to have id, name and description
    String id = query.getId();
    if(id == null || (id != null && id.isEmpty())) {
      throw new MetadataServiceException("Application id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry =
        mds.get(context, accountId, null,
                FieldTypes.Query.ID, id);

      // If stream does not exist, then no point in deleting it.
      if(readEntry == null) {
        return true;
      }

      // Invoke MDS to delete entry.
      mds.delete(context, accountId, null, FieldTypes.Query.ID, id);
    } catch (OperationException e) {
      Log.warn("Failed deleting query {}. Reason : {}",
               query, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
  }

  /**
   * Returns a list of query associated with account.
   *
   * @param account for which list of queries need to be retrieved.
   * @throws com.continuuity.metadata.thrift.MetadataServiceException
   *          thrown when there is issue listing
   *          queries for a account.
   * @returns a list of queries associated with account; else empty list.
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
    Collection<MetaDataEntry> queries;
    try {
      queries = mds.list(context, accountId, null, FieldTypes.Query.ID, null);
    } catch (OperationException e) {
      Log.warn("Failed to list queries for account {}. Reason: {}.",
          account, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }

    for(MetaDataEntry entry : queries) {
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
        mds.get(context, accountId, app, FieldTypes.Application.ID, id);

      if(entry != null) {
        query = makeQuery(entry);
      } else {
        query.setExists(false);
      }
    } catch (OperationException e) {
      Log.warn("Failed to retrieve query {}. Reason : {}.",
               query, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return query;
  }

  /**
   *  When creating/updating a flow, you need to have id, name and application
   */
  void validateFlow(Flow flow) throws MetadataServiceException {
    String id = flow.getId();
    if(id == null || id.isEmpty()) {
      throw new MetadataServiceException("Flow id is empty or null.");
    }
    String name = flow.getName();
    if(name == null || name.isEmpty()) {
      throw new MetadataServiceException("Flow name is empty or null.");
    }
    String app = flow.getName();
    if(app == null || app.isEmpty()) {
      throw new MetadataServiceException("Flow's app name is empty or null.");
    }
  }

  String ListToString(List<String> list) {
    StringBuilder str = new StringBuilder();
    if (list != null) {
      for (String item : list) {
        str.append(item);
        str.append(' ');
      }
    }
    return str.toString();
  }

  List<String> StringToList(String str) {
    if (str == null || str.isEmpty())
      return Collections.emptyList();
    StringTokenizer tok = new StringTokenizer(str, " ");
    List<String> list = Lists.newArrayList();
    while (tok.hasMoreTokens()) {
      list.add(tok.nextToken());
    }
    return list;
  }

  private MetaDataEntry makeEntry(String account, Flow flow) {
    // Create a new metadata entry.
    MetaDataEntry entry = new MetaDataEntry(
        account, flow.getApplication(), FieldTypes.Flow.ID, flow.getId());
    entry.addField(FieldTypes.Flow.NAME, flow.getName());
    entry.addField(FieldTypes.Flow.STREAMS, ListToString(flow.getStreams()));
    entry.addField(FieldTypes.Flow.DATASETS, ListToString(flow.getDatasets()));
    return entry;
  }

  private Flow makeFlow(MetaDataEntry entry) {
    return new Flow(entry.getId(), entry.getApplication(),
        entry.getTextField(FieldTypes.Flow.NAME),
        StringToList(entry.getTextField(FieldTypes.Flow.STREAMS)),
        StringToList(entry.getTextField(FieldTypes.Flow.DATASETS)));
  }

  @Override
  public boolean createFlow(String account, Flow flow) throws
      MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    // validate flow
    validateFlow(flow);

    // create a context
    OperationContext opContext =
        new OperationContext(account, flow.getApplication());

    // perform the insert
    try {
      this.mds.add(opContext, makeEntry(account, flow));
      return true;
    } catch (OperationException e) {
      if (e.getStatus() == StatusCode.WRITE_CONFLICT) // entry already exists
        return false;
      Log.warn("Failed to create flow {}. Reason: {}.", flow, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
  }

  @Override
  public boolean updateFlow(String account, Flow flow)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    // validate flow
    validateFlow(flow);

    // create a context
    OperationContext opContext =
        new OperationContext(account, flow.getApplication());

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

    // first get all flows for the app
    List<Flow> flows = getFlowsByApplication(account, app);

    // this will hold all the datasets we find in flows
    Map<String, Dataset> foundDatasets = Maps.newHashMap();

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
