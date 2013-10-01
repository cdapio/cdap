package com.continuuity.metadata;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.metadata.types.Application;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.types.Flow;
import com.continuuity.metadata.types.Mapreduce;
import com.continuuity.metadata.types.Procedure;
import com.continuuity.metadata.types.Stream;
import com.continuuity.metadata.types.Workflow;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;

// todo: is all the complex retry logic still required? Or can we simplify this?
// todo: is this layer needed at all? Or should it all be moved to he app-fabric's MDSBasedStore?

/**
 * Implementation of thrift meta data service handler.
 */
public class MetaDataStore extends MetadataHelper {

  private static final Logger LOG = LoggerFactory.getLogger(MetaDataStore.class);

  private final MetaDataTable mds;

  /**
   * Construction of metadata service handler.
   * @param mds an instance of a meta data store
   */
  @Inject
  public MetaDataStore(MetaDataTable mds) {
    this.mds = mds;
  }

  //-------------------- generic methods ---------------------------------

  /**
   * Creates a new meta data entry, fails if an entry for the same object
   * already exists.
   *
   * Relies on the meta data helper for the specific type of meta data
   * objects to do conversions, comparisons, etc. All other code is generic
   * for all types of objects.
   *
   * @param t the meta data object to create in the MDS.
   * @return true if successful; false otherwise
   * @throws MetadataServiceException when there is a failure.
   */
  private <T> boolean create(Helper<T> helper, String account, T t)
      throws MetadataServiceException {

    // Validate account and meta data object
    validateAccount(account);
    helper.validate(t);

    // Create a context.
    OperationContext context = new OperationContext(account);

    try {
      // perform the insert, no conflict resolution
      this.mds.add(context, helper.makeEntry(account, t), false);
      return true;
    } catch (OperationException e) {
      String message = String.format("Failed to create %s %s. Reason: %s.",
          helper.getName(), helper.getId(t), e.getMessage());
      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }

  }


  /**
   * Creates a meta data object if not existing. If the object already
   * exists, verify that the existing entry is compatible (equivalent or
   * subsuming) with the new entry. In other words, after this method
   * succeeds, a compatible entry is guaranteed to exist.
   *
   * Relies on the meta data helper for the specific type of meta data
   * objects to do conversions, comparisons, etc. All other code is generic
   * for all types of objects.
   *
   * @param t the meta data object to create in the MDS.
   * @return true if successful; false otherwise
   * @throws MetadataServiceException when there is a failure.
   */
  private <T> boolean assertt(Helper<T> helper, String account, T t)
      throws MetadataServiceException {

    // Validate account and meta data object
    validateAccount(account);
    helper.validate(t);

    // Create a context.
    OperationContext context = new OperationContext(account);

    try {
      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry = mds.get(context, account,
          helper.getApplication(t), helper.getFieldType(), helper.getId(t));
      if (readEntry == null) {
        // attempt to add, but in case of write conflict we must read
        // again and try to resolve the conflict
        MetaDataEntry entry = helper.makeEntry(account, t);
        try {
          // Invoke MDS to add entry
          mds.add(context, entry);
          return true;
        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
            throw e; // we can only handle write conflicts here
          }
          // read again for conflict resolution
          readEntry = mds.get(context, account,
              helper.getApplication(t), helper.getFieldType(), helper.getId(t));
        }
      }

      // loop a few times for write conflict resolution
      for (int attempts = 3; attempts > 0; --attempts) {
        // there is already an entry, determine how it compare to the new one
        CompareStatus status = readEntry == null
            ? CompareStatus.SUPER : helper.compare(t, readEntry);
        // existing entry is equal or a superset of the new one -> good
        if (status.equals(CompareStatus.EQUAL) ||
            status.equals(CompareStatus.SUB)) {
          return true;
        }

        if (status.equals(CompareStatus.DIFF)) {
          // new entry is incompatible with existing -> conflict!
          throw new MetadataServiceException("another, incompatible meta " +
              "data entry already exists.");
        }

        // Create a new metadata entry for update
        MetaDataEntry entry = helper.makeEntry(account, t);
        try {
          // Invoke MDS to update entry, this can again fail with write conflict
          if (readEntry == null) {
            mds.add(context, entry);
          } else {
            mds.update(context, entry);
          }
          return true;

        } catch (OperationException e) {
          if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
            throw e; // we can only handle write conflicts here
          }

          if (attempts <= 1) {
            throw e; // number of attempts exhausted
          }

          // read again for conflict resolution
          readEntry = mds.get(context, account,
              helper.getApplication(t), helper.getFieldType(), helper.getId(t));
        }
      }
      // unreachable but java does not detect that
      throw new OperationException(StatusCode.INTERNAL_ERROR,
          "this code should be unreachable");

    } catch (OperationException e) {
      String message = String.format("Failed creating %s '%s'. Reason: %s",
          helper.getName(), t, e.getMessage());
      LOG.error(message);
      throw new MetadataServiceException(message);
    }
  }

  /**
   * Updates an existing meta data object.
   * Relies on the meta data helper for the specific type of meta data objects
   * to do conversions, comparisons, etc. All other code is generic for all
   * types of objects.
   *
   * @param t the meta data object to create in the MDS.
   * @return true if the entry existed and delete successful; false if the
   *    entry did not exist.
   * @throws MetadataServiceException when there is a failure.
   */
  private <T> boolean update(Helper<T> helper, String account, T t)
      throws MetadataServiceException {

    // Validate account and the meta data object.
    validateAccount(account);
    helper.validate(t);

    // create a context
    OperationContext opContext =
        new OperationContext(account, helper.getApplication(t));

    // perform the update
    try {
      this.mds.update(opContext, helper.makeEntry(account, t));
      return true;
    } catch (OperationException e) {
      if (e.getStatus() == StatusCode.ENTRY_NOT_FOUND) { // entry does not exist
        return false;
      }
      String message = String.format("Failed to update %s %s. Reason: %s.",
          helper.getName(), helper.getId(t), e.getMessage());
      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }
  }

  /**
   * Deletes a meta data object if existing. Relies on the meta data helper
   * for the specific type of meta data objects to do conversions, comparisons,
   * etc. All other code is generic for all types of objects.
   *
   * @param app the application of the meta data object. Null if not associated with an app.
   * @param id the meta data object to delete in the MDS.
   * @return true if successful; false otherwise
   * @throws MetadataServiceException when there is a failure.
   */
  private <T> boolean delete(Helper<T> helper, String account, String app, String id)
      throws MetadataServiceException {

    // Validate all account.
    validateAccount(account);

    // Verify the meta data object has an id
    if (id == null || id.isEmpty()) {
      throw new MetadataServiceException(helper.getName() +
          " id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(account);
      if (app == null) {
        LOG.debug(String.format(
            "Deleting meta data for %s '%s' in account '%s'.", helper.getName(), id, account));
      } else {
        LOG.debug(String.format(
            "Deleting meta data for %s '%s' in account '%s' and application '%s'.",
            helper.getName(), id, account, app));
      }
      // Invoke MDS to delete entry.
      // This will also succeed if the entry does not exist
      mds.delete(context, account, app, helper.getFieldType(), id);
      return true;

    } catch (OperationException e) {
      String message = String.format("Failed deleting %s '%s'. Reason: %s", helper.getName(), id, e.getMessage());
      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }
  }

  /**
   * Retrieve all meta data objects of a given type associated with an account.
   * Relies on the meta data helper for the specific type of meta data
   * objects to do conversions, comparisons, etc. All other code is generic
   * for all types of objects.
   *
   * @param account for which objects need to be retrieved.
   * @return list of objects associated with account.
   * @throws MetadataServiceException in case of a failure
   */
  private <T> List<T> list(Helper<T> helper, String account, String app)
      throws MetadataServiceException {

    // Validate all account.
    validateAccount(account);

    List<T> result = Lists.newArrayList();

    try {
      // Create a context.
      OperationContext context = new OperationContext(account);

      // Invoke MDS to list streams for an account.
      // note: application may be null, and filter fields are null
      Collection<MetaDataEntry> entries =
          mds.list(context, account, app, helper.getFieldType(), null);

      for (MetaDataEntry entry : entries) {
        result.add(helper.makeFromEntry(entry));
      }
      return result;

    } catch (OperationException e) {
      String message = String.format(
          "Failed listing %s's for account %s. Reason: %s", helper.getName(), account, e.getMessage());
      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }
  }

  /**
   * Returns a single meta data object
   * Relies on the meta data helper for the specific type of meta data
   * objects to do conversions, comparisons, etc. All other code is generic
   * for all types of objects.
   *
   * @param account account to which the object belongs to.
   * @param app the application of the meta data object. Null if not associated with an app.
   * @param id the id of the object for which more information is requested.
   * @return if the object exists in the MDS, the object with additional
   *    information like name and description. Otherwise an empty meta data
   *    objects with exists = false.
   * @throws MetadataServiceException in case of failure
   */
  private <T> T get(Helper<T> helper, String account, String app, String id)
      throws MetadataServiceException {

    // Validate account.
    validateAccount(account);

    if (id == null || id.isEmpty()) {
      throw new MetadataServiceException(
          helper.getName() + " does not have an id.");
    }

    try {
      OperationContext context = new OperationContext(account);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry = mds.get(context, account, app, helper.getFieldType(), id);

      if (entry != null) {
        // convert the the meta data entry
        return helper.makeFromEntry(entry);
      } else {
        return null;
      }
    } catch (OperationException e) {
      String message = String.format("Failed to retrieve %s %s. Reason: %s.", helper.getName(), id, e.getMessage());
      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }
  }

  //-------------------------- Stream APIs ---------------------------------

  public boolean createStream(String account, Stream stream)
      throws MetadataServiceException {
    return create(streamHelper, account, stream);
  }

  public boolean assertStream(String account, Stream stream)
      throws MetadataServiceException {
    return assertt(streamHelper, account, stream);
  }

  public boolean deleteStream(String account, String stream)
      throws MetadataServiceException {
    return delete(streamHelper, account, null, stream);
  }

  public List<Stream> getStreams(String account)
      throws MetadataServiceException {
    return list(streamHelper, account, null);
  }

  public Stream getStream(String account, String stream)
      throws MetadataServiceException {
    return get(streamHelper, account, null, stream);
  }

  //-------------------------- Dataset APIs ---------------------------------

  public boolean createDataset(String account, Dataset dataset) throws
      MetadataServiceException {
    return create(datasetHelper, account, dataset);
  }

  public boolean assertDataset(String account, Dataset dataset) throws
      MetadataServiceException {
    return assertt(datasetHelper, account, dataset);
  }

  public boolean deleteDataset(String account, String dataset) throws
      MetadataServiceException {
    return delete(datasetHelper, account, null, dataset);
  }

  public List<Dataset> getDatasets(String account) throws
      MetadataServiceException {
    return list(datasetHelper, account, null);
  }

  public Dataset getDataset(String account, String dataset)
      throws MetadataServiceException {
    return get(datasetHelper, account, null, dataset);
  }

  //---------------------- Application APIs --------------------------------

  public boolean createApplication(String account, Application application)
      throws MetadataServiceException {
    return create(applicationHelper, account, application);
  }

  public boolean updateApplication(String account, Application application)
    throws MetadataServiceException {
    return update(applicationHelper, account, application);
  }

  public boolean deleteApplication(String account, String application)
      throws MetadataServiceException {
    return delete(applicationHelper, account, null, application);
  }

  public List<Application> getApplications(String account)
      throws MetadataServiceException {
    return list(applicationHelper, account, null);
  }

  public Application getApplication(String account, String application)
      throws MetadataServiceException {
    return get(applicationHelper, account, null, application);
  }

  //-------------------------- Procedure APIs --------------------------------

  public boolean createProcedure(String account, Procedure procedure)
      throws MetadataServiceException {
    return create(procedureHelper, account, procedure);
  }

  public boolean updateProcedure(String account, Procedure procedure)
      throws MetadataServiceException {
    return update(procedureHelper, account, procedure);
  }

  public boolean deleteProcedure(String account, String app, String procedure)
      throws MetadataServiceException {
    return delete(procedureHelper, account, app, procedure);
  }

  // clean: used in app fabric to maintain this
  public List<Procedure> getProcedures(String account)
      throws MetadataServiceException {
    return list(procedureHelper, account, null);
  }

  public List<Procedure> getProceduresByApplication(String account, String appid)
      throws MetadataServiceException {
    return list(procedureHelper, account, appid);
  }

  // clean: used only in test
  public Procedure getProcedure(String account, String app, String procedure)
      throws MetadataServiceException {
    return get(procedureHelper, account, app, procedure);
  }

  //-------------------------- Mapreduce APIs --------------------------------

  public boolean createMapreduce(String account, Mapreduce mapreduce)
      throws MetadataServiceException {
    return create(mapreduceHelper, account, mapreduce);
  }

  public boolean updateMapreduce(String account, Mapreduce mapreduce)
      throws MetadataServiceException {
    return update(mapreduceHelper, account, mapreduce);
  }

  public boolean deleteMapreduce(String account, String app, String mapreduce)
      throws MetadataServiceException {
    return delete(mapreduceHelper, account, app, mapreduce);
  }

  public List<Mapreduce> getMapreduces(String account)
      throws MetadataServiceException {
    return list(mapreduceHelper, account, null);
  }

  public List<Mapreduce> getMapreducesByApplication(String account, String appid)
      throws MetadataServiceException {
    return list(mapreduceHelper, account, appid);
  }

  public Mapreduce getMapreduce(String account, String app, String mapreduce)
      throws MetadataServiceException {
    return get(mapreduceHelper, account, app, mapreduce);
  }

  //-------------------------- Flow APIs --------------------------------

  // clean: used only in app-fabric to maintain flows here
  public boolean createFlow(String accountId, Flow flow) throws
      MetadataServiceException {
    return create(flowHelper, accountId, flow);
  }

  // clean: used only in app-fabric to maintain flows here
  public boolean updateFlow(String accountId, Flow flow) throws
      MetadataServiceException {
    return update(flowHelper, accountId, flow);
  }

  // clean: used by app-fabric to maintain flows here
  public boolean deleteFlow(String account, String appid, String flowid)
      throws MetadataServiceException {
    return delete(flowHelper, account, appid, flowid);
  }

  // clean: used here for getFlowsByStream/Dataset
  public List<Flow> getFlows(String account)
      throws MetadataServiceException {
    return list(flowHelper, account, null);
  }

  // clean: used by gateway, used in this class to get all flows by stream/dataset,
  //        used in app-fabric only for maintenance of flows here
  public List<Flow> getFlowsByApplication(String account, String application)
      throws MetadataServiceException {
    return list(flowHelper, account, application);
  }

  // clean: only used in test
  public Flow getFlow(String account, String application, String flowid)
      throws MetadataServiceException {
    return get(flowHelper, account, application, flowid);
  }

  //---------------------------Workflow apis -----------------------------------

  public boolean createWorkflow(String accountId, Workflow workflow) throws
    MetadataServiceException {
    return create(workflowHelper, accountId, workflow);
  }

  // clean: used by app-fabric to maintain this, used by this to clear all
  public List<Workflow> getWorkflows(String account) throws MetadataServiceException {
    return list(workflowHelper, account, null);
  }

  public boolean deleteWorkflow(String account, String app, String workflowId)
    throws MetadataServiceException {
    return delete(workflowHelper, account, app, workflowId);
  }

  public boolean updateWorkflow(String accountId, Workflow workflow) throws
    MetadataServiceException {
    return update(workflowHelper, accountId, workflow);
  }

  public void deleteAll(String account)
      throws MetadataServiceException {

    LOG.info("Deleting all meta data for account '" + account + "'.");
    // Validate account.
    validateAccount(account);

    // list all streams for the account and delete them
    for (Stream stream : getStreams(account)) {
      deleteStream(account, stream.getId());
    }
    LOG.info("Stream meta data for account '" + account + "' deleted.");

    // list all datasets for the account and delete them
    for (Dataset dataset : getDatasets(account)) {
      deleteDataset(account, dataset.getId());
    }
    LOG.info("Dataset meta data for account '" + account + "' deleted.");

    // list all queries for the account and delete them
    for (Procedure procedure : getProcedures(account)) {
      deleteProcedure(account, procedure.getApplication(), procedure.getId());
    }
    LOG.info("Procedure meta data for account '" + account + "' deleted.");

    // list all mapreduces for the account and delete them
    for (Mapreduce mapreduce : getMapreduces(account)) {
      deleteMapreduce(account, mapreduce.getApplication(), mapreduce.getId());
    }
    LOG.info("Mapreduce meta data for account '" + account + "' deleted.");

    // list all flows for the account and delete them
    for (Flow flow : getFlows(account)) {
      deleteFlow(account, flow.getApplication(), flow.getId());
    }
    LOG.info("Flow meta data for account '" + account + "' deleted.");

    for (Workflow workflow : getWorkflows(account)) {
      deleteWorkflow(account, workflow.getApplication(), workflow.getId());
    }
    LOG.info("Workflow meta data for account '" + account + "' deleted.");

    // list all applications for the account and delete them
    for (Application application : getApplications(account)) {
      deleteApplication(account, application.getId());
    }
    LOG.info("Application meta data for account '" + account + "' deleted.");
    LOG.info("All meta data for account '" + account + "' deleted.");
  }

}
