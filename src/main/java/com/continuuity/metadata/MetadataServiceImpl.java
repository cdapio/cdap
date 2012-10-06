package com.continuuity.metadata;

import com.continuuity.api.data.*;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.metadata.stubs.*;
import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import org.mortbay.log.Log;

import java.util.Collection;
import java.util.List;

/**
 * Implementation of thrift meta data service handler.
 */
public class MetadataServiceImpl implements MetadataService.Iface {
  private final MetaDataStore mds;

  /**
   * Construction of metadata service handler
   * @param opex instance of opex.
   */
  public MetadataServiceImpl(OperationExecutor opex) {
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
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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

    if(! stream.isSetDescription()) {
      throw new MetadataServiceException("Stream description should be set " +
                                           "for create");
    }
    String description = stream.getDescription();
    if(description == null || (description != null && description.isEmpty())) {
      throw new MetadataServiceException("Stream description is empty or null");
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
   * @param stream
   * @return true if successfull; false otherwise
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
   * @param account
   * @return list of stream associated with account.
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
   * @param dataset
   * @return true if successfull; false otherwise
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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

    if(! dataset.isSetDescription()) {
      throw new MetadataServiceException("Dataset description should be set " +
                                           "for create");
    }

    String description = dataset.getDescription();
    if(description == null || (description != null && description.isEmpty())) {
      throw new MetadataServiceException("Stream description is empty or null");
    }

    if(! dataset.isSetType()) {
      throw new MetadataServiceException("Dataset type should be set for create");
    }
    DatasetType type = dataset.getType();

    if(type == null) {
      throw new MetadataServiceException("Dataset type cannot be null");
    }

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
      entry.addField(FieldTypes.Dataset.TYPE, type.name());
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
   * @param dataset
   * @return true if successfull; false otherwise.
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
      mds.delete(context, accountId, null, FieldTypes.Stream.ID, id);
    } catch (OperationException e) {
      Log.warn("Failed deleting dataset {}. Reason : {}", dataset, e.getMessage());
      throw new MetadataServiceException(e.getMessage());
    }
    return true;
  }

  /**
   * @param account
   * @return list of data set associated with account
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
        rDataset.setType(DatasetType.valueOf(
          dataset.getTextField(FieldTypes.Dataset.TYPE)
        ));
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
   * @param account
   * @param dataset
   * @return Dataset
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
   * @param account
   * @param application
   * @return true if created successfully or already exists, false otherwise.
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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

    if(! application.isSetName()) {
      throw new MetadataServiceException("Application name should be set for create");
    }
    String name = application.getName();
    if(name == null || (name != null && name.isEmpty())) {
      throw new MetadataServiceException("Application name cannot be null or empty");
    }

    if(! application.isSetDescription()) {
      throw new MetadataServiceException("Application description should be set " +
                                           "for create");
    }
    String description = application.getDescription();
    if(description == null || (description != null && description.isEmpty())) {
      throw new MetadataServiceException("Application description is empty or null");
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
   * @param account
   * @param application
   * @return true if application was deleted successfully or did not exists to
   *         be deleted; false otherwise.
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
   * @param account
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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
   * @param account
   * @param application
   * @return application meta data if exists; else the id passed.
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
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

    String accountId = account.getId();

    if(accountId == null || accountId.isEmpty()) {
      throw new
        MetadataServiceException("Account Id cannot be null or empty");
    }
  }
}
