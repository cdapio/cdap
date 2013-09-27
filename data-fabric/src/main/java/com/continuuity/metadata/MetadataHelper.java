package com.continuuity.metadata;

import com.continuuity.metadata.types.Application;
import com.continuuity.metadata.types.Dataset;
import com.continuuity.metadata.types.Flow;
import com.continuuity.metadata.types.Mapreduce;
import com.continuuity.metadata.types.Procedure;
import com.continuuity.metadata.types.Stream;
import com.continuuity.metadata.types.Workflow;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Supporting class for managing MDS service.
 */
public class MetadataHelper {

  //-------------- Some utilities for list/string conversion -----------------

  static String listToString(List<String> list) {
    StringBuilder str = new StringBuilder();
    if (list != null) {
      for (String item : list) {
        str.append(item);
        str.append(' ');
      }
    }
    return str.toString();
  }

  static List<String> stringToList(String str) {
    if (str == null || str.isEmpty()) {
      return Collections.emptyList();
    }
    StringTokenizer tok = new StringTokenizer(str, " ");
    List<String> list = Lists.newArrayList();
    while (tok.hasMoreTokens()) {
      list.add(tok.nextToken());
    }
    return list;
  }

  //-------------- Some utilities to compare meta data -----------------------

  enum CompareStatus {
    EQUAL, DIFF, SUPER, SUB
  }

  // returns SUPER if the new value has more information than the existing one.
  static CompareStatus compareAlso(CompareStatus soFar,
                                   String newValue,
                                   String existingValue) {
    if (soFar.equals(CompareStatus.DIFF)) {
      return soFar;
    }

    if (newValue == null) {
      // both null, no change in status
      if (existingValue == null) {
        return soFar;
      }

      // new value has less info: incompatible if it had more info so far
      if (soFar.equals(CompareStatus.SUPER)) {
        return CompareStatus.DIFF;
      }

      // new value has less info and it did not have more so far -> sub
      return CompareStatus.SUB;
    } else { // new != null
      // both are the same, no change in status
      if (newValue.equals(existingValue)) {
        return soFar;
      }

      // both non-null but different
      if (existingValue != null) {
        return CompareStatus.DIFF;
      }

      // new value has more info: incompatible if it had less info so far
      if (soFar.equals(CompareStatus.SUB)) {
        return CompareStatus.DIFF;
      }
      // new value has more info and it did not have less so far -> super
      return CompareStatus.SUPER;
    }
  }

  // returns SUPER if the new value has more information than the existing one.
  static CompareStatus compareAlso(CompareStatus soFar,
                                   List<String> newValue,
                                   List<String> existingValue) {
    if (soFar.equals(CompareStatus.DIFF)) {
      return soFar;
    }

    if (newValue == null) {
      // both null, no change in status
      if (existingValue == null) {
        return soFar;
      }
      // new value has less info: incompatible if it had more info so far
      if (soFar.equals(CompareStatus.SUPER)) {
        return CompareStatus.DIFF;
      }
      // new value has less info and it did not have more so far -> sub
      return CompareStatus.SUB;
    } else { // new != null
      // both are the same, no change in status
      if (newValue.equals(existingValue)) {
        return soFar;
      }
      // both non-null but different
      if (existingValue != null) {
        return CompareStatus.DIFF;
      }
      // new value has more info: incompatible if it had less info so far
      if (soFar.equals(CompareStatus.SUB)) {
        return CompareStatus.DIFF;
      }

      // new value has more info and it did not have less so far -> super
      return CompareStatus.SUPER;
    }
  }

  // returns SUPER if the new value has more information than the existing one.
  static CompareStatus compareAlso(CompareStatus soFar, Long newValue, Long existingValue) {
    if (soFar.equals(CompareStatus.DIFF)) {
      return soFar;
    }

    if (newValue == null) {
      // both null, no change in status
      if (existingValue == null) {
        return soFar;
      }

      // new value has less info: incompatible if it had more info so far
      if (soFar.equals(CompareStatus.SUPER)) {
        return CompareStatus.DIFF;
      }
      // new value has less info and it did not have more so far -> sub
      return CompareStatus.SUB;
    } else { // new != null
      // both are the same, no change in status
      if (newValue.equals(existingValue)) {
        return soFar;
      }
      // both non-null but different
      if (existingValue != null) {
        return CompareStatus.DIFF;
      }
      // new value has more info: incompatible if it had less info so far
      if (soFar.equals(CompareStatus.SUB)) {
        return CompareStatus.DIFF;
      }
      // new value has more info and it did not have less so far -> super
      return CompareStatus.SUPER;
    }
  }

  //-------------------------- Account stuff ----------------------------------

  /**
   * Validates the account passed.
   *
   * @param account to be validated.
   */
  void validateAccount(String account) {
    Preconditions.checkNotNull(account, "Account id must not be null");
    Preconditions.checkArgument(!account.isEmpty(), "Account id must not be empty");
  }

  //-------------------------- Generic stuff ----------------------------------

  /**
   * Generic class to manage meta data objects. It helps the meta data service
   * generic methods to deal with the type specific conversion, comparison etc.
   */
  interface Helper<T> {

    /** validate the completeness of a meta object to be written. */
    public void validate(T t);

    /** convert a raw mds entry into a meta object of the specific type. */
    public MetaDataEntry makeEntry(String account, T t);

    /** convert a meta object into a raw mds entry. */
    public T makeFromEntry(MetaDataEntry entry);

    /** compare a meta object with an existing raw meta entry. */
    public CompareStatus compare(T t, MetaDataEntry existingEntry);

    /** get the id of a meta object. */
    public String getId(T t);

    /** get the application of a meta object. May return null. */
    public String getApplication(T t);

    /** get the name for this type of objects, e.g., "stream". */
    public String getName();

    /** get the type to use for the raw mds objects. */
    public String getFieldType();
  }

  // static helpers, one for each meta data type
  static Helper<Stream> streamHelper = new StreamHelper();
  static Helper<Dataset> datasetHelper = new DatasetHelper();
  static Helper<Application> applicationHelper = new ApplicationHelper();
  static Helper<Procedure> procedureHelper = new ProcedureHelper();
  static Helper<Flow> flowHelper = new FlowHelper();
  static Helper<Mapreduce> mapreduceHelper = new MapreduceHelper();
  static Helper<Workflow> workflowHelper = new WorkflowHelper();

  //-------------------------- Stream stuff ----------------------------------

  static class StreamHelper implements Helper<Stream> {

    public static final String ID = "stream";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String CAPACITY_IN_BYTES = "capacityInBytes";
    public static final String EXPIRY_IN_SECONDS = "expiryInSeconds";

    @Override
    public void validate(Stream stream) {
      // When creating a stream, you need to have id, name and description
      Preconditions.checkNotNull(stream.getId(), "Stream id must not be null");
      Preconditions.checkArgument(!stream.getId().isEmpty(), "Stream id must not be empty");
      Preconditions.checkNotNull(stream.getName(), "Stream name must not be null");
      Preconditions.checkArgument(!stream.getName().isEmpty(), "Stream name must not be empty");
    }

    @Override
    public MetaDataEntry makeEntry(String account, Stream stream) {
      MetaDataEntry entry = new MetaDataEntry(
          account, null, ID, stream.getId());
      if (stream.getName() != null) {
        entry.addField(NAME, stream.getName());
      }

      if (stream.getDescription() != null) {
        entry.addField(DESCRIPTION, stream.getDescription());
      }

      if (stream.getCapacityInBytes() != null) {
        entry.addField(CAPACITY_IN_BYTES,
            String.format("%d", stream.getCapacityInBytes()));
      }

      if (stream.getExpiryInSeconds() != null) {
        entry.addField(EXPIRY_IN_SECONDS,
                       String.format("%d", stream.getExpiryInSeconds()));
      }

      return entry;
    }

    @Override
    public Stream makeFromEntry(MetaDataEntry entry) {
      Stream stream = new Stream(entry.getId());
      String name = entry.getTextField(NAME);
      if (name != null) {
        stream.setName(name);
      }

      String description = entry.getTextField(DESCRIPTION);
      if (description != null) {
        stream.setDescription(description);
      }

      String capacity = entry.getTextField(CAPACITY_IN_BYTES);
      if (capacity != null) {
        stream.setCapacityInBytes(Integer.valueOf(capacity));
      }

      String expiry = entry.getTextField(EXPIRY_IN_SECONDS);
      if (expiry != null) {
        stream.setExpiryInSeconds(Integer.valueOf(expiry));
      }
      return stream;
    }

    @Override
    public CompareStatus compare(Stream stream, MetaDataEntry existingEntry) {

      Stream existing = makeFromEntry(existingEntry);
      CompareStatus status = CompareStatus.EQUAL;

      status = compareAlso(status, stream.getId(), existing.getId());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, stream.getName(), existing.getName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(
          status, stream.getDescription(), existing.getDescription());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, stream.getCapacityInBytes(), existing.getCapacityInBytes());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, stream.getExpiryInSeconds(), existing.getExpiryInSeconds());
      return status;
    }

    @Override
    public String getId(Stream stream) {
      return stream.getId();
    }

    @Override
    public String getApplication(Stream stream) {
      return null;
    }

    @Override
    public String getName() {
      return "stream";
    }

    @Override
    public String getFieldType() {
      return ID;
    }
  } // end StreamHelper

  //-------------------------- Dataset stuff ---------------------------------

  static class DatasetHelper implements Helper<Dataset> {

    public static final String ID = "dataset";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";
    public static final String SPECIFICATION = "specification";
    public static final String TYPE = "type";

    @Override
    public void validate(Dataset dataset) {
      Preconditions.checkNotNull(dataset.getId(), "Dataset id must not be null");
      Preconditions.checkArgument(!dataset.getId().isEmpty(), "Dataset id must not be empty");
      Preconditions.checkNotNull(dataset.getName(), "Dataset name must not be null");
      Preconditions.checkArgument(!dataset.getName().isEmpty(), "Dataset name must not be empty");
      Preconditions.checkNotNull(dataset.getType(), "Dataset type must not be null");
      Preconditions.checkArgument(!dataset.getType().isEmpty(), "Dataset type must not be empty");
    }

    @Override
    public MetaDataEntry makeEntry(String account, Dataset dataset) {
      MetaDataEntry entry = new MetaDataEntry(
          account, null, ID, dataset.getId());
      if (dataset.getName() != null) {
        entry.addField(NAME, dataset.getName());
      }

      if (dataset.getDescription() != null) {
        entry.addField(DESCRIPTION, dataset.getDescription());
      }

      if (dataset.getType() != null) {
        entry.addField(TYPE, dataset.getType());
      }

      if (dataset.getSpecification() != null) {
        entry.addField(SPECIFICATION, dataset.getSpecification());
      }
      return entry;
    }

    @Override
    public Dataset makeFromEntry(MetaDataEntry entry) {
      Dataset dataset = new Dataset(entry.getId());

      String name = entry.getTextField(NAME);
      if (name != null) {
        dataset.setName(name);
      }

      String description = entry.getTextField(DESCRIPTION);
      if (description != null) {
        dataset.setDescription(description);
      }

      String type = entry.getTextField(TYPE);
      if (type != null) {
        dataset.setType(type);
      }

      String spec = entry.getTextField(SPECIFICATION);
      if (spec != null) {
        dataset.setSpecification(spec);
      }
      return dataset;
    }

    @Override
    public CompareStatus compare(Dataset dataset, MetaDataEntry existingEntry) {
      Dataset existing = makeFromEntry(existingEntry);
      CompareStatus status = CompareStatus.EQUAL;

      status = compareAlso(status, dataset.getId(), existing.getId());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, dataset.getName(), existing.getName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(
          status, dataset.getDescription(), existing.getDescription());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, dataset.getType(), existing.getType());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, dataset.getSpecification(), existing.getSpecification());
      return status;
    }

    @Override
    public String getId(Dataset dataset) {
      return dataset.getId();
    }

    @Override
    public String getApplication(Dataset dataset) {
      return null;
    }

    @Override
    public String getName() {
      return "dataset";
    }

    @Override
    public String getFieldType() {
      return ID;
    }

  } // end DatasetHelper

  //-------------------------- Application stuff -----------------------------

  static class ApplicationHelper implements Helper<Application> {

    public static final String ID = "application";
    public static final String NAME = "name";
    public static final String DESCRIPTION = "description";

    @Override
    public void validate(Application app) {
      Preconditions.checkNotNull(app.getId(), "Application id must not be null");
      Preconditions.checkArgument(!app.getId().isEmpty(), "Application id must not be empty");
      Preconditions.checkNotNull(app.getName(), "Application name must not be null");
      Preconditions.checkArgument(!app.getName().isEmpty(), "Application name must not be empty");
    }

    @Override
    public MetaDataEntry makeEntry(String account, Application app) {
      MetaDataEntry entry = new MetaDataEntry(
          account, null, ID, app.getId());
      if (app.getName() != null) {
        entry.addField(NAME, app.getName());
      }
      if (app.getDescription() != null) {
        entry.addField(DESCRIPTION,
                       app.getDescription());
      }
      return entry;
    }

    @Override
    public Application makeFromEntry(MetaDataEntry entry) {
      Application app = new Application(entry.getId());

      String name = entry.getTextField(DatasetHelper.NAME);
      if (name != null) {
        app.setName(name);
      }

      String description = entry.getTextField(DatasetHelper.DESCRIPTION);
      if (description != null) {
        app.setDescription(description);
      }
      return app;
    }

    @Override
    public CompareStatus compare(Application app, MetaDataEntry existingEntry) {
      Application existing = makeFromEntry(existingEntry);
      CompareStatus status = CompareStatus.EQUAL;

      status = compareAlso(status, app.getId(), existing.getId());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, app.getName(), existing.getName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(
          status, app.getDescription(), existing.getDescription());
      return status;
    }

    @Override
    public String getId(Application application) {
      return application.getId();
    }

    @Override
    public String getApplication(Application application) {
      return null;
    }

    @Override
    public String getName() {
      return "application";
    }

    @Override
    public String getFieldType() {
      return ID;
    }

  } // end ApplicationHelper

  //-------------------------- Procedure stuff -----------------------------------

  static class ProcedureHelper implements Helper<Procedure> {

    public static final String ID = "procedure";
    public static final String NAME = "name";
    public static final String DATASETS = "datasets";
    public static final String DESCRIPTION = "description";
    public static final String SERVICE_NAME = "serviceName";

    @Override
    public void validate(Procedure procedure) {
      Preconditions.checkNotNull(procedure.getId(), "Procedure id must not be null");
      Preconditions.checkArgument(!procedure.getId().isEmpty(), "Procedure id must not be empty");
      Preconditions.checkNotNull(procedure.getName(), "Procedure name must not be null");
      Preconditions.checkArgument(!procedure.getName().isEmpty(), "Procedure name must not be empty");
      Preconditions.checkNotNull(procedure.getApplication(), "Application name must not be null");
      Preconditions.checkArgument(!procedure.getApplication().isEmpty(), "Application name must not be empty");
      Preconditions.checkNotNull(procedure.getServiceName(), "Service name must not be null");
      Preconditions.checkArgument(!procedure.getServiceName().isEmpty(), "Service name must not be empty");
   }

    @Override
    public MetaDataEntry makeEntry(String account, Procedure procedure) {
      MetaDataEntry entry = new MetaDataEntry(
        account, procedure.getApplication(), ID, procedure.getId());

      if (procedure.getName() != null) {
        entry.addField(NAME, procedure.getName());
      }

      if (procedure.getDescription() != null) {
        entry.addField(DESCRIPTION, procedure.getDescription());
      }

      if (procedure.getServiceName() != null) {
        entry.addField(SERVICE_NAME, procedure.getServiceName());
      }

      if (procedure.getDatasets() != null) {
        entry.addField(DATASETS,
                       listToString(procedure.getDatasets()));
      }

      return entry;
    }

    @Override
    public Procedure makeFromEntry(MetaDataEntry entry) {
      Procedure procedure = new Procedure(entry.getId(), entry.getApplication());

      String name = entry.getTextField(NAME);
      if (name != null) {
        procedure.setName(name);
      }

      String description = entry.getTextField(DESCRIPTION);
      if (description != null) {
        procedure.setDescription(description);
      }

      String service = entry.getTextField(SERVICE_NAME);
      if (service != null) {
        procedure.setServiceName(service);
      }

      String datasets = entry.getTextField(DATASETS);
      if (datasets != null) {
        procedure.setDatasets(stringToList(datasets));
      }
      return procedure;
    }

    @Override
    public CompareStatus compare(Procedure procedure, MetaDataEntry existingEntry) {
      Procedure existing = makeFromEntry(existingEntry);
      CompareStatus status = CompareStatus.EQUAL;

      status = compareAlso(status, procedure.getId(), existing.getId());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, procedure.getName(), existing.getName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(
          status, procedure.getDescription(), existing.getDescription());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, procedure.getServiceName(),
          existing.getServiceName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, procedure.getDatasets(), existing.getDatasets());
      return status;
    }

    @Override
    public String getId(Procedure procedure) {
      return procedure.getId();
    }

    @Override
    public String getApplication(Procedure procedure) {
      return procedure.getApplication();
    }

    @Override
    public String getName() {
      return "procedure";
    }

    @Override
    public String getFieldType() {
      return ID;
    }

  } // end ProcedureHelper

  //-------------------------- Mapreduce stuff -----------------------------------

  static class MapreduceHelper implements Helper<Mapreduce> {

    public static final String ID = "mapreduce";
    public static final String NAME = "name";
    public static final String DATASETS = "datasets";
    public static final String DESCRIPTION = "description";

    @Override
    public void validate(Mapreduce mapreduce) {
      Preconditions.checkNotNull(mapreduce.getId(), "Mapreduce id must not be null");
      Preconditions.checkArgument(!mapreduce.getId().isEmpty(), "Mapreduce id must not be empty");
      Preconditions.checkNotNull(mapreduce.getName(), "Mapreduce name must not be null");
      Preconditions.checkArgument(!mapreduce.getName().isEmpty(), "Mapreduce name must not be empty");
      Preconditions.checkNotNull(mapreduce.getApplication(), "Application name must not be null");
      Preconditions.checkArgument(!mapreduce.getApplication().isEmpty(), "Application name must not be empty");
    }

    @Override
    public MetaDataEntry makeEntry(String account, Mapreduce mapreduce) {
      MetaDataEntry entry = new MetaDataEntry(account,
          mapreduce.getApplication(), ID, mapreduce.getId());
      if (mapreduce.getName() != null) {
        entry.addField(NAME, mapreduce.getName());
      }
      if (mapreduce.getDescription() != null) {
        entry.addField(DESCRIPTION, mapreduce.getDescription());
      }
      if (mapreduce.getDatasets() != null) {
        entry.addField(DATASETS,
            listToString(mapreduce.getDatasets()));
      }
      return entry;
    }

    @Override
    public Mapreduce makeFromEntry(MetaDataEntry entry) {
      Mapreduce mapreduce = new Mapreduce(entry.getId(), entry.getApplication());
      String name = entry.getTextField(NAME);
      if (name != null) {
        mapreduce.setName(name);
      }
      String description = entry.getTextField(DESCRIPTION);
      if (description != null) {
        mapreduce.setDescription(description);
      }
      String datasets = entry.getTextField(DATASETS);
      if (datasets != null) {
        mapreduce.setDatasets(stringToList(datasets));
      }
      return mapreduce;
    }

    @Override
    public CompareStatus compare(Mapreduce mapreduce, MetaDataEntry existingEntry) {
      Mapreduce existing = makeFromEntry(existingEntry);
      CompareStatus status = CompareStatus.EQUAL;
      status = compareAlso(status, mapreduce.getId(), existing.getId());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }
      status = compareAlso(status, mapreduce.getName(), existing.getName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }
      status = compareAlso(
          status, mapreduce.getDescription(), existing.getDescription());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }
      status = compareAlso(status, mapreduce.getDatasets(), existing.getDatasets());
      return status;
    }

    @Override
    public String getId(Mapreduce mapreduce) {
      return mapreduce.getId();
    }

    @Override
    public String getApplication(Mapreduce mapreduce) {
      return mapreduce.getApplication();
    }

    @Override
    public String getName() {
      return "mapreduce";
    }

    @Override
    public String getFieldType() {
      return ID;
    }

  } // end MapreduceHelper

  //-------------------------- Flow stuff ------------------------------------

  static class FlowHelper implements Helper<Flow> {

    public static final String ID = "flow";
    public static final String NAME = "name";
    public static final String STREAMS = "streams";
    public static final String DATASETS = "datasets";

    @Override
    public void validate(Flow flow) {
      Preconditions.checkNotNull(flow.getId(), "Flow id must not be null");
      Preconditions.checkArgument(!flow.getId().isEmpty(), "Flow id must not be empty");
      Preconditions.checkNotNull(flow.getName(), "Flow name must not be null");
      Preconditions.checkArgument(!flow.getName().isEmpty(), "Flow name must not be empty");
      Preconditions.checkNotNull(flow.getApplication(), "Application name must not be null");
      Preconditions.checkArgument(!flow.getApplication().isEmpty(), "Application name must not be empty");
    }

    @Override
    public MetaDataEntry makeEntry(String account, Flow flow) {
      // Create a new metadata entry.
      MetaDataEntry entry = new MetaDataEntry(account, flow.getApplication(), ID, flow.getId());
      if (flow.getName() != null) {
        entry.addField(NAME, flow.getName());
      }
      entry.addField(STREAMS, listToString(flow.getStreams()));
      entry.addField(DATASETS, listToString(flow.getDatasets()));
      return entry;
    }

    @Override
    public Flow makeFromEntry(MetaDataEntry entry) {
      Flow fl = new Flow(entry.getId(), entry.getApplication());
      if (entry.getTextField(NAME) != null) {
        fl.setName(entry.getTextField(NAME));
      }
      fl.setStreams(stringToList(entry.getTextField(STREAMS)));
      fl.setDatasets(stringToList(entry.getTextField(DATASETS)));
      return fl;
    }

    @Override
    public CompareStatus compare(Flow flow, MetaDataEntry existingEntry) {
      Flow existing = makeFromEntry(existingEntry);
      CompareStatus status = CompareStatus.EQUAL;

      status = compareAlso(status, flow.getId(), existing.getId());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, flow.getName(), existing.getName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, flow.getDatasets(), existing.getDatasets());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, flow.getStreams(), existing.getStreams());
      return status;
    }

    @Override
    public String getId(Flow flow) {
      return flow.getId();
    }

    @Override
    public String getApplication(Flow flow) {
      return flow.getApplication();
    }

    @Override
    public String getName() {
      return "flow";
    }

    @Override
    public String getFieldType() {
      return ID;
    }

  } // end FlowHelper

  //-------------------------- Workflow stuff -------------------------------

  static class WorkflowHelper implements Helper<Workflow> {

    public static final String ID = "workflow";
    public static final String NAME = "name";

    @Override
    public void validate(Workflow workflow) {
      Preconditions.checkNotNull(workflow.getId(), "Workflow id must not be null");
      Preconditions.checkArgument(!workflow.getId().isEmpty(), "Workflow id must not be empty");
      Preconditions.checkNotNull(workflow.getName(), "Workflow name must not be null");
      Preconditions.checkArgument(!workflow.getName().isEmpty(), "Workflow name must not be empty");
      Preconditions.checkNotNull(workflow.getApplication(), "Application name must not be null");
      Preconditions.checkArgument(!workflow.getApplication().isEmpty(), "Application name must not be empty");
    }

    @Override
    public MetaDataEntry makeEntry(String account, Workflow workflow) {
      // Create a new metadata entry.
      MetaDataEntry entry = new MetaDataEntry(account,
                                              workflow.getApplication(), ID, workflow.getId());
      entry.addField(NAME, workflow.getName());
      return entry;
    }

    @Override
    public Workflow makeFromEntry(MetaDataEntry entry) {
      Workflow fl = new Workflow(entry.getId(), entry.getApplication());
      fl.setName(entry.getTextField(NAME));
      return fl;
    }

    @Override
    public CompareStatus compare(Workflow workflow, MetaDataEntry existingEntry) {
      Workflow existing = makeFromEntry(existingEntry);
      CompareStatus status = CompareStatus.EQUAL;

      status = compareAlso(status, workflow.getId(), existing.getId());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      status = compareAlso(status, workflow.getName(), workflow.getName());
      if (status.equals(CompareStatus.DIFF)) {
        return status;
      }

      return status;
    }

    @Override
    public String getId(Workflow flow) {
      return flow.getId();
    }

    @Override
    public String getApplication(Workflow flow) {
      return flow.getApplication();
    }

    @Override
    public String getName() {
      return "workflow";
    }

    @Override
    public String getFieldType() {
      return ID;
    }
  }
}
