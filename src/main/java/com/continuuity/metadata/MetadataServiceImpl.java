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

    // Validate all account.
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
                FieldTypes.Stream.ID, id);
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
                FieldTypes.Stream.ID, id);

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
