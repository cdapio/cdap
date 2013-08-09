package com.continuuity.metadata;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.metadata.SerializingMetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.data.operation.StatusCode;
import com.continuuity.data.operation.executor.OperationExecutor;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Application;
import com.continuuity.metadata.thrift.Dataset;
import com.continuuity.metadata.thrift.Flow;
import com.continuuity.metadata.thrift.Mapreduce;
import com.continuuity.metadata.thrift.MetadataServiceException;
import com.continuuity.metadata.thrift.Query;
import com.continuuity.metadata.thrift.Stream;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Implementation of thrift meta data service handler.
 */
public class MetadataService extends MetadataHelper
    implements com.continuuity.metadata.thrift.MetadataService.Iface {

  private static final Logger LOG = LoggerFactory.getLogger(MetadataService.class);

  private final MetaDataStore mds;

  /**
   * Construction of metadata service handler.
   * @param opex instance of opex.
   */
  @Inject
  public MetadataService(OperationExecutor opex) {
    this.mds = new SerializingMetaDataStore(opex);
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
  private <T> boolean create(Helper<T> helper, Account account, T t)
      throws MetadataServiceException, TException {

    // Validate account and meta data object
    validateAccount(account);
    helper.validate(t);

    // Create a context.
    OperationContext context = new OperationContext(account.getId());

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
  private <T> boolean assertt(Helper<T> helper, Account account, T t)
      throws MetadataServiceException, TException {

    // Validate account and meta data object
    validateAccount(account);
    helper.validate(t);

    // Create a context.
    OperationContext context = new OperationContext(account.getId());

    try {
      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry readEntry = mds.get(context, account.getId(),
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
          readEntry = mds.get(context, account.getId(),
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
          readEntry = mds.get(context, account.getId(),
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
  private <T> boolean update(Helper<T> helper, Account account, T t)
      throws MetadataServiceException, TException {

    // Validate account and the meta data object.
    validateAccount(account);
    helper.validate(t);

    // create a context
    OperationContext opContext =
        new OperationContext(account.getId(), helper.getApplication(t));

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
   * @param t the meta data object to delete in the MDS.
   * @return true if successful; false otherwise
   * @throws MetadataServiceException when there is a failure.
   */
  private <T> boolean delete(Helper<T> helper, Account account, T t)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();

    // Verify the meta data object has an id
    String id = helper.getId(t);
    if (id == null || id.isEmpty()) {
      throw new MetadataServiceException(helper.getName() +
          " id is empty or null.");
    }

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);
      if (helper.getApplication(t) == null) {
        LOG.debug(String.format(
            "Deleting meta data for %s '%s' in account '%s'.",
            helper.getName(), helper.getId(t), account.getId()));
      } else {
        LOG.debug(String.format(
            "Deleting meta data for %s '%s' in account '%s' and application " +
            "'%s'.", helper.getName(), helper.getId(t), account.getId(),
            helper.getApplication(t)));
      }
      // Invoke MDS to delete entry.
      // This will also succeed if the entry does not exist
      mds.delete(context,
          accountId, helper.getApplication(t), helper.getFieldType(), id);
      return true;

    } catch (OperationException e) {
      String message = String.format("Failed deleting %s %s. Reason: %s",
          helper.getName(), t, e.getMessage());
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
  private <T> List<T> list(Helper<T> helper, Account account, Application app)
      throws MetadataServiceException, TException {

    // Validate all account.
    validateAccount(account);
    String accountId = account.getId();
    String appId = app == null ? null : app.getId();

    List<T> result = Lists.newArrayList();

    try {
      // Create a context.
      OperationContext context = new OperationContext(accountId);

      // Invoke MDS to list streams for an account.
      // note: application may be null, and filter fields are null
      Collection<MetaDataEntry> entries =
          mds.list(context, accountId, appId, helper.getFieldType(), null);

      for (MetaDataEntry entry : entries) {
        result.add(helper.makeFromEntry(entry));
      }
      return result;

    } catch (OperationException e) {
      String message = String.format(
          "Failed listing %s's for account %s. Reason: %s",
          helper.getName(), accountId, e.getMessage());
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
   * @param t the object for which more information is requested.
   * @return if the object exists in the MDS, the object with additional
   *    information like name and description. Otherwise an empty meta data
   *    objects with exists = false.
   * @throws MetadataServiceException in case of failure
   */
  private <T> T get(Helper<T> helper, Account account, T t)
      throws MetadataServiceException, TException {

    // Validate account.
    validateAccount(account);
    String accountId = account.getId();

    String id = helper.getId(t);
    if (id == null || id.isEmpty()) {
      throw new MetadataServiceException(
          helper.getName() + " does not have an id.");
    }

    try {
      OperationContext context = new OperationContext(accountId);

      // Read the meta data entry to see if it's already present.
      // If already present, return without applying the new changes.
      MetaDataEntry entry = mds.get(context,
          accountId, helper.getApplication(t), helper.getFieldType(), id);

      if (entry != null) {
        // convert the the meta data entry
        return helper.makeFromEntry(entry);
      } else {
        // return a blank object with exists = false
        return helper.makeNonExisting(t);
      }
    } catch (OperationException e) {
      String message = String.format("Failed to retrieve %s %s. Reason: %s.",
          helper.getName(), helper.getId(t), e.getMessage());
      LOG.error(message, e);
      throw new MetadataServiceException(message);
    }
  }

  private boolean addItemToListField(
      String account, String app,
      String type, String id, String what,
      String field, String item) throws MetadataServiceException, TException {

    // validate account.
    validateAccount(account);

    // create a context
    OperationContext opContext = new OperationContext(account, app);

    LOG.debug(String.format("Adding '%s' to field %s of %s '%s'...",
        item, field, what, id));

    // try n times, give up after n write conflicts
    final int retryAttempts = 5;
    for (int attempts = retryAttempts; attempts >= 0; --attempts) {
      // retrieve the meta data entry
      MetaDataEntry entry;
      try {
        entry = this.mds.get(opContext, account, app, type, id);
      } catch (OperationException e) {
        String message = String.format("Failed to get %s '%s' for account " +
            "'%s'. Reason: %s.", what, id, account, e.getMessage());
        LOG.error(message, e);
        throw new MetadataServiceException(message);
      }

      if (entry == null) {
        throw new MetadataServiceException(
          "No meta data found for " + what + " '" + id + "'");
      }

      String oldValue = entry.getTextField(field);
      for (String x : oldValue.split(" ")) {
        if (x.equals(item)) {
          LOG.debug(String.format("No need to add '%s' to field %s of %s " +
              "'%s': Already in current value '%s'.", item, field, what, id,
              oldValue));
          return true; // item is already in list
        }
      }
      String newValue = oldValue == null ? item + " " : oldValue + item + " ";

      try {
        mds.swapField(opContext, account, app, type, id, field,
            oldValue, newValue, -1);
        LOG.debug(String.format("Added '%s' to field %s of %s '%s': " +
            "New value is '%s'.", item, field, what, id, newValue));
        return true;
      } catch (OperationException e) {
        String message = String.format("Failed to swap field '%s' for %s " +
            "'%s'. Reason: %s.", field, what, id, e.getMessage());
        if (e.getStatus() != StatusCode.WRITE_CONFLICT) {
          // not a write conflict, must be some more serious problem
          LOG.error(message);
          throw new MetadataServiceException(message);
        }
        if (attempts <= 0) {
          // retry attempts exhausted, bail out
          LOG.error(message, e);
          message = String.format("Repeatedly failed to swap field '%s' for " +
              "%s (%d attempts). Giving up.", field, what, retryAttempts);
          throw new MetadataServiceException(message);
        }
        // there was a write conflict, random sleep before next attempt
        LOG.debug(message);
        int millis = new Random(System.currentTimeMillis()).nextInt(100);
        LOG.debug("Sleeping " + millis + " ms before next attempt.");
        try {
          Thread.sleep(millis);
        } catch (InterruptedException ie) {
          message = "InterruptedException during sleep()";
          LOG.error(message);
          throw new MetadataServiceException(message);
        }
      }
    }
    // must be a write conflict, repeatedly, but this statement will not be
    // reached...
    return false;
  }

  //-------------------------- Stream APIs ---------------------------------

  @Override
  public boolean createStream(Account account, Stream stream)
      throws MetadataServiceException, TException {
    return create(streamHelper, account, stream);
  }

  @Override
  public boolean assertStream(Account account, Stream stream)
      throws MetadataServiceException, TException {
    return assertt(streamHelper, account, stream);
  }

  @Override
  public boolean deleteStream(Account account, Stream stream)
      throws MetadataServiceException, TException {
    return delete(streamHelper, account, stream);
  }

  @Override
  public List<Stream> getStreams(Account account)
      throws MetadataServiceException, TException {
    return list(streamHelper, account, null);
  }

  @Override
  public Stream getStream(Account account, Stream stream)
      throws MetadataServiceException, TException {
    return get(streamHelper, account, stream);
  }

  //-------------------------- Dataset APIs ---------------------------------

  @Override
  public boolean createDataset(Account account, Dataset dataset) throws
      MetadataServiceException, TException {
    return create(datasetHelper, account, dataset);
  }

  @Override
  public boolean assertDataset(Account account, Dataset dataset) throws
      MetadataServiceException, TException {
    return assertt(datasetHelper, account, dataset);
  }

  @Override
  public boolean deleteDataset(Account account, Dataset dataset) throws
      MetadataServiceException, TException {
    return delete(datasetHelper, account, dataset);
  }

  @Override
  public List<Dataset> getDatasets(Account account) throws
      MetadataServiceException, TException {
    return list(datasetHelper, account, null);
  }

  @Override
  public Dataset getDataset(Account account, Dataset dataset)
      throws MetadataServiceException, TException {
    return get(datasetHelper, account, dataset);
  }

  //---------------------- Application APIs --------------------------------

  @Override
  public boolean createApplication(Account account, Application application)
      throws MetadataServiceException, TException {
    return create(applicationHelper, account, application);
  }

  @Override
  public boolean updateApplication(Account account, Application application)
    throws MetadataServiceException, TException {
    return update(applicationHelper, account, application);
  }

  @Override
  public boolean deleteApplication(Account account, Application application)
      throws MetadataServiceException, TException {
    return delete(applicationHelper, account, application);
  }

  @Override
  public List<Application> getApplications(Account account)
      throws MetadataServiceException, TException {
    return list(applicationHelper, account, null);
  }

  @Override
  public Application getApplication(Account account, Application application)
      throws MetadataServiceException, TException {
    return get(applicationHelper, account, application);
  }

  //-------------------------- Query APIs --------------------------------

  @Override
  public boolean createQuery(Account account, Query query)
      throws MetadataServiceException, TException {
    return create(queryHelper, account, query);
  }

  @Override
  public boolean updateQuery(Account account, Query query)
      throws MetadataServiceException, TException {
    return update(queryHelper, account, query);
  }

  @Override
  public boolean addDatasetToQuery(String account, String app,
                                   String qid, String dataset)
      throws MetadataServiceException, TException {
    return addItemToListField(account, app, FieldTypes.Query.ID, qid,
                              "query", FieldTypes.Query.DATASETS, dataset);
  }

  @Override
  public boolean deleteQuery(Account account, Query query)
      throws MetadataServiceException, TException {
    return delete(queryHelper, account, query);
  }

  @Override
  public List<Query> getQueries(Account account)
      throws MetadataServiceException, TException {
    return list(queryHelper, account, null);
  }

  @Override
  public List<Query> getQueriesByApplication(String account, String appid)
      throws MetadataServiceException, TException {
    return list(queryHelper, new Account(account), new Application(appid));
  }

  @Override
  public Query getQuery(Account account, Query query)
      throws MetadataServiceException, TException {
    return get(queryHelper, account, query);
  }

  //-------------------------- Mapreduce APIs --------------------------------

  @Override
  public boolean createMapreduce(Account account, Mapreduce mapreduce)
      throws MetadataServiceException, TException {
    return create(mapreduceHelper, account, mapreduce);
  }

  @Override
  public boolean updateMapreduce(Account account, Mapreduce mapreduce)
      throws MetadataServiceException, TException {
    return update(mapreduceHelper, account, mapreduce);
  }

  @Override
  public boolean addDatasetToMapreduce(String account, String app,
                                   String qid, String dataset)
      throws MetadataServiceException, TException {
    return addItemToListField(account, app, FieldTypes.Mapreduce.ID, qid,
        "mapreduce", FieldTypes.Mapreduce.DATASETS, dataset);
  }

  @Override
  public boolean deleteMapreduce(Account account, Mapreduce mapreduce)
      throws MetadataServiceException, TException {
    return delete(mapreduceHelper, account, mapreduce);
  }

  @Override
  public List<Mapreduce> getMapreduces(Account account)
      throws MetadataServiceException, TException {
    return list(mapreduceHelper, account, null);
  }

  @Override
  public List<Mapreduce> getMapreducesByApplication(String account, String appid)
      throws MetadataServiceException, TException {
    return list(mapreduceHelper, new Account(account), new Application(appid));
  }

  @Override
  public Mapreduce getMapreduce(Account account, Mapreduce mapreduce)
      throws MetadataServiceException, TException {
    return get(mapreduceHelper, account, mapreduce);
  }

  //-------------------------- Flow APIs --------------------------------

  @Override
  public boolean createFlow(String accountId, Flow flow) throws
      MetadataServiceException, TException {
    return create(flowHelper, new Account(accountId), flow);
  }

  @Override
  public boolean updateFlow(String accountId, Flow flow) throws
      MetadataServiceException, TException {
    return update(flowHelper, new Account(accountId), flow);
  }

  @Override
  public boolean addDatasetToFlow(String account, String app,
                                  String flowid, String dataset)
      throws MetadataServiceException, TException {
    return addItemToListField(account, app, FieldTypes.Flow.ID, flowid,
        "flow", FieldTypes.Flow.DATASETS, dataset);
  }

  @Override
  public boolean addStreamToFlow(String account, String app,
                                 String flowid, String stream)
      throws MetadataServiceException, TException {
    return addItemToListField(account, app, FieldTypes.Flow.ID, flowid,
        "flow", FieldTypes.Flow.STREAMS, stream);
  }

  @Override
  public boolean deleteFlow(String account, String appid, String flowid)
      throws MetadataServiceException, TException {
    return delete(flowHelper, new Account(account), new Flow(flowid, appid));
  }

  @Override
  public List<Flow> getFlows(String account)
      throws MetadataServiceException, TException {
    return list(flowHelper, new Account(account), null);
  }

  @Override
  public List<Flow> getFlowsByApplication(String account, String application)
      throws MetadataServiceException, TException {
    return list(flowHelper, new Account(account), new Application(application));
  }

  @Override
  public Flow getFlow(String account, String application, String flowid)
      throws MetadataServiceException, TException {
    return get(flowHelper, new Account(account), new Flow(flowid, application));
  }

  //----------- Queries that require joining across meta data types ----------

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
      if (flowStreams == null || flowStreams.isEmpty()) {
        continue;
      }
      for (String streamName : flowStreams) {
        if (foundStreams.containsKey(streamName)) {
          continue;
        }
        Stream stream =
            getStream(new Account(account), new Stream(streamName));
        if (stream.isExists()) {
          foundStreams.put(streamName, stream);
        }
      }
    }

    // extract the found streams into a list
    List<Stream> streams = Lists.newArrayList();
    for (Stream stream : foundStreams.values()) {
      streams.add(stream);
    }
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
      if (flowDatasets == null || flowDatasets.isEmpty()) {
        continue;
      }
      for (String datasetName : flowDatasets) {
        if (foundDatasets.containsKey(datasetName)) {
          continue;
        }
        Dataset dataset =
            getDataset(new Account(account), new Dataset(datasetName));
        if (dataset.isExists()) {
          foundDatasets.put(datasetName, dataset);
        }
      }
    }

    // first get all queries for the app
    List<Query> queries = getQueriesByApplication(account, app);

    // now iterate over all queries and get each dataset
    for (Query query : queries) {
      List<String> queryDatasets = query.getDatasets();
      if (queryDatasets == null || queryDatasets.isEmpty()) {
        continue;
      }
      for (String datasetName : queryDatasets) {
        if (foundDatasets.containsKey(datasetName)) {
          continue;
        }
        Dataset dataset =
            getDataset(new Account(account), new Dataset(datasetName));
        if (dataset.isExists()) {
          foundDatasets.put(datasetName, dataset);
        }
      }
    }

    // first get all mapreduces for the app
    List<Mapreduce> mapreduces = getMapreducesByApplication(account, app);

    // now iterate over all flows and get each dataset
    for (Mapreduce mapreduce : mapreduces) {
      List<String> mapreduceDatasets = mapreduce.getDatasets();
      if (mapreduceDatasets == null || mapreduceDatasets.isEmpty()) {
        continue;
      }
      for (String datasetName : mapreduceDatasets) {
        if (foundDatasets.containsKey(datasetName)) {
          continue;
        }
        Dataset dataset =
            getDataset(new Account(account), new Dataset(datasetName));
        if (dataset.isExists()) {
          foundDatasets.put(datasetName, dataset);
        }
      }
    }

    // extract the found datasets into a list
    List<Dataset> datasets = Lists.newArrayList();
    for (Dataset dataset : foundDatasets.values()) {
      datasets.add(dataset);
    }
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
      if (flow.getStreams().contains(stream)) {
        flowsForStream.add(flow);
      }
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
      if (flow.getDatasets().contains(dataset)) {
        flowsForDS.add(flow);
      }
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
      if (query.getDatasets().contains(dataset)) {
        queriesForDS.add(query);
      }
    }
    return queriesForDS;
  }

  @Override
  public List<Mapreduce> getMapreducesByDataset(String account, String dataset)
      throws MetadataServiceException, TException {
    // Validate all account.
    validateAccount(account);

    // first get all flows for the app
    List<Mapreduce> mapreduces = getMapreduces(new Account(account));

    // select the flows that use the dataset
    List<Mapreduce> queriesForDS = Lists.newLinkedList();
    for (Mapreduce mapreduce : mapreduces) {
      if (mapreduce.getDatasets().contains(dataset)) {
        queriesForDS.add(mapreduce);
      }
    }
    return queriesForDS;
  }

  @Override
  public void deleteAll(String accountId)
      throws MetadataServiceException, TException {

    LOG.info("Deleting all meta data for account '" + accountId + "'.");
    // Validate account.
    validateAccount(accountId);
    Account account = new Account(accountId);

    // list all streams for the account and delete them
    for (Stream stream : getStreams(account)) {
      deleteStream(account, stream);
    }
    LOG.info("Stream meta data for account '" + accountId + "' deleted.");

    // list all datasets for the account and delete them
    for (Dataset dataset : getDatasets(account)) {
      deleteDataset(account, dataset);
    }
    LOG.info("Dataset meta data for account '" + accountId + "' deleted.");

    // list all queries for the account and delete them
    for (Query query : getQueries(account)) {
      deleteQuery(account, query);
    }
    LOG.info("Query meta data for account '" + accountId + "' deleted.");

    // list all mapreduces for the account and delete them
    for (Mapreduce mapreduce : getMapreduces(account)) {
      deleteMapreduce(account, mapreduce);
    }
    LOG.info("Mapreduce meta data for account '" + accountId + "' deleted.");

    // list all flows for the account and delete them
    for (Flow flow : getFlows(accountId)) {
      deleteFlow(accountId, flow.getApplication(), flow.getId());
    }
    LOG.info("Flow meta data for account '" + accountId + "' deleted.");

    // list all applications for the account and delete them
    for (Application application : getApplications(account)) {
      deleteApplication(account, application);
    }
    LOG.info("Application meta data for account '" + accountId + "' deleted.");
    LOG.info("All meta data for account '" + accountId + "' deleted.");
  }

}
