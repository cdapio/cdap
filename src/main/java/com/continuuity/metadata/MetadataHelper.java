package com.continuuity.metadata;

import com.continuuity.api.data.MetaDataEntry;
import com.continuuity.metadata.thrift.*;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

public class MetadataHelper {

  //-------------- Some utilities for list/string conversion -----------------

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

  //-------------- Some utilities to compare meta data -----------------------

  enum CompareStatus {
    EQUAL, DIFF, SUPER, SUB
  }

  // returns SUPER if the new value has more information than the existing one
  CompareStatus compareAlso(CompareStatus soFar,
                            String newValue,
                            String existingValue) {
    if (soFar.equals(CompareStatus.DIFF)) return soFar;
    if (newValue == null) {
      // both null, no change in status
      if (existingValue == null) return soFar;
        // new value has less info: incompatible if it had more info so far
      else if (soFar.equals(CompareStatus.SUPER)) return CompareStatus.DIFF;
        // new value has less info and it did not have more so far -> sub
      else return CompareStatus.SUB;
    } else { // new != null
      // both are the same, no change in status
      if (newValue.equals(existingValue)) return soFar;
        // both non-null but different
      else if (existingValue != null) return CompareStatus.DIFF;
        // new value has more info: incompatible if it had less info so far
      else if (soFar.equals(CompareStatus.SUB)) return CompareStatus.DIFF;
        // new value has more info and it did not have less so far -> super
      else return CompareStatus.SUPER;
    }
  }

  // returns SUPER if the new value has more information than the existing one
  CompareStatus compareAlso(CompareStatus soFar,
                            List<String> newValue,
                            List<String> existingValue) {
    if (soFar.equals(CompareStatus.DIFF)) return soFar;
    if (newValue == null) {
      // both null, no change in status
      if (existingValue == null) return soFar;
        // new value has less info: incompatible if it had more info so far
      else if (soFar.equals(CompareStatus.SUPER)) return CompareStatus.DIFF;
        // new value has less info and it did not have more so far -> sub
      else return CompareStatus.SUB;
    } else { // new != null
      // both are the same, no change in status
      if (newValue.equals(existingValue)) return soFar;
        // both non-null but different
      else if (existingValue != null) return CompareStatus.DIFF;
        // new value has more info: incompatible if it had less info so far
      else if (soFar.equals(CompareStatus.SUB)) return CompareStatus.DIFF;
        // new value has more info and it did not have less so far -> super
      else return CompareStatus.SUPER;
    }
  }

  // returns SUPER if the new value has more information than the existing one
  CompareStatus compareAlso(CompareStatus soFar,
                            boolean newNull, long newValue,
                            boolean existingNull, long existingValue) {
    if (soFar.equals(CompareStatus.DIFF)) return soFar;
    if (newNull) {
      // both null, no change in status
      if (existingNull) return soFar;
        // new value has less info: incompatible if it had more info so far
      else if (soFar.equals(CompareStatus.SUPER)) return CompareStatus.DIFF;
        // new value has less info and it did not have more so far -> sub
      else return CompareStatus.SUB;
    } else { // new != null
      // both are the same, no change in status
      if (newValue == existingValue) return soFar;
        // both non-null but different
      else if (!existingNull) return CompareStatus.DIFF;
        // new value has more info: incompatible if it had less info so far
      else if (soFar.equals(CompareStatus.SUB)) return CompareStatus.DIFF;
        // new value has more info and it did not have less so far -> super
      else return CompareStatus.SUPER;
    }
  }

  //-------------------------- Stream stuff ----------------------------------

  // When creating a stream, you need to have id, name and description
  void validateStream(Stream stream) throws MetadataServiceException {
    if(stream.getId() == null || stream.getId().isEmpty()) {
      throw new MetadataServiceException("Stream id is empty or null.");
    }
    if(stream.getName() == null || stream.getName().isEmpty()) {
      throw new MetadataServiceException(
          "Stream name must not be null or empty");
    }
  }

  MetaDataEntry makeEntry(Account account, Stream stream) {
    MetaDataEntry entry = new MetaDataEntry(
        account.getId(), null, FieldTypes.Stream.ID, stream.getId());
    if (stream.isSetName())
      entry.addField(FieldTypes.Stream.NAME, stream.getName());
    if (stream.isSetDescription())
      entry.addField(FieldTypes.Stream.DESCRIPTION, stream.getDescription());
    if(stream.isSetCapacityInBytes())
      entry.addField(FieldTypes.Stream.CAPACITY_IN_BYTES,
          String.format("%d", stream.getCapacityInBytes()));
    if(stream.isSetExpiryInSeconds())
      entry.addField(FieldTypes.Stream.EXPIRY_IN_SECONDS,
          String.format("%d", stream.getExpiryInSeconds()));

    return entry;
  }

  Stream makeStream(MetaDataEntry entry) {
    Stream stream = new Stream(entry.getId());
    String name = entry.getTextField(FieldTypes.Stream.NAME);
    if (name != null) stream.setName(name);
    String description = entry.getTextField(FieldTypes.Stream.DESCRIPTION);
    if (description != null) stream.setDescription(description);
    String capacity = entry.getTextField(FieldTypes.Stream.CAPACITY_IN_BYTES);
    if (capacity != null) stream.setCapacityInBytes(Integer.valueOf(capacity));
    String expiry = entry.getTextField(FieldTypes.Stream.EXPIRY_IN_SECONDS);
    if (expiry != null) stream.setExpiryInSeconds(Integer.valueOf(expiry));
    return stream;
  }

  CompareStatus compare(Stream stream, MetaDataEntry existingEntry) {
    Stream existing = makeStream(existingEntry);
    CompareStatus status = CompareStatus.EQUAL;
    status = compareAlso(status, stream.getId(), existing.getId());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, stream.getName(), existing.getName());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(
        status, stream.getDescription(), existing.getDescription());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status,
        stream.isSetCapacityInBytes(), stream.getCapacityInBytes(),
        existing.isSetCapacityInBytes(), existing.getCapacityInBytes());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status,
        stream.isSetExpiryInSeconds(), stream.getExpiryInSeconds(),
        existing.isSetExpiryInSeconds(), existing.getExpiryInSeconds());
    return status;
  }

  //-------------------------- Dataset stuff ---------------------------------

  // When creating a dataset, you need to have id, name and description
  void validateDataset(Dataset dataset) throws MetadataServiceException {
    if (dataset.getId() == null || dataset.getId().isEmpty()) {
      throw new MetadataServiceException("Dataset id is empty or null.");
    }
    if (dataset.getName() == null || dataset.getName().isEmpty()) {
      throw new MetadataServiceException(
          "Dataset name must not be empty or null for create.");
    }
    if (dataset.getType() == null || dataset.getType().isEmpty()) {
      throw new MetadataServiceException(
          "Dataset type must not be empty or null for create.");
    }
  }

  MetaDataEntry makeEntry(Account account, Dataset dataset) {
    MetaDataEntry entry = new MetaDataEntry(
        account.getId(), null, FieldTypes.Dataset.ID, dataset.getId());
    if (dataset.isSetName())
      entry.addField(FieldTypes.Dataset.NAME, dataset.getName());
    if (dataset.isSetDescription())
      entry.addField(FieldTypes.Dataset.DESCRIPTION, dataset.getDescription());
    if (dataset.isSetType())
      entry.addField(FieldTypes.Dataset.TYPE, dataset.getType());
    return entry;
  }

  Dataset makeDataset(MetaDataEntry entry) {
    Dataset dataset = new Dataset(entry.getId());
    String name = entry.getTextField(FieldTypes.Dataset.NAME);
    if (name != null) dataset.setName(name);
    String description = entry.getTextField(FieldTypes.Dataset.DESCRIPTION);
    if (description != null) dataset.setDescription(description);
    String type = entry.getTextField(FieldTypes.Dataset.TYPE);
    if (type != null) dataset.setType(type);
    return dataset;
  }

  CompareStatus compare(Dataset dataset, MetaDataEntry existingEntry) {
    Dataset existing = makeDataset(existingEntry);
    CompareStatus status = CompareStatus.EQUAL;
    status = compareAlso(status, dataset.getId(), existing.getId());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, dataset.getName(), existing.getName());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(
        status, dataset.getDescription(), existing.getDescription());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, dataset.getType(), existing.getType());
    return status;
  }

  //-------------------------- Application stuff -----------------------------

  // When creating an application, you need to have id and name
  void validateApplication(Application app) throws MetadataServiceException {
    if (app.getId() == null || app.getId().isEmpty()) {
      throw new MetadataServiceException("Application id is empty or null.");
    }
    if (app.getName() == null || app.getName().isEmpty()) {
      throw new MetadataServiceException("" +
          "Application name cannot be null or empty for create.");
    }
  }

  MetaDataEntry makeEntry(Account account, Application app) {
    MetaDataEntry entry = new MetaDataEntry(
        account.getId(), null, FieldTypes.Application.ID, app.getId());
    if (app.isSetName())
      entry.addField(FieldTypes.Application.NAME, app.getName());
    if (app.isSetDescription())
      entry.addField(FieldTypes.Application.DESCRIPTION,
          app.getDescription());
    return entry;
  }

  Application makeApplication(MetaDataEntry entry) {
    Application app = new Application(entry.getId());
    String name = entry.getTextField(FieldTypes.Dataset.NAME);
    if (name != null) app.setName(name);
    String description = entry.getTextField(FieldTypes.Dataset.DESCRIPTION);
    if (description != null) app.setDescription(description);
    return app;
  }

  CompareStatus compare(Application app, MetaDataEntry existingEntry) {
    Application existing = makeApplication(existingEntry);
    CompareStatus status = CompareStatus.EQUAL;
    status = compareAlso(status, app.getId(), existing.getId());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, app.getName(), existing.getName());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(
        status, app.getDescription(), existing.getDescription());
    return status;
  }

  //-------------------------- Query stuff -----------------------------------

  // When creating a query, you need to have id, name and app
  void validateQuery(Query query) throws MetadataServiceException {
    if (query.getId() == null || query.getId().isEmpty())
      throw new MetadataServiceException("Query id is empty or null.");
    if (query.getName() == null || query.getName().isEmpty())
      throw new MetadataServiceException("Query name is empty or null.");
    if(query.getApplication() == null || query.getApplication().isEmpty())
      throw new MetadataServiceException("Query's app name is empty or null.");
    if(query.getServiceName() == null || query.getServiceName().isEmpty())
      throw new MetadataServiceException(
          "Query service name cannot be null or empty");
  }

  MetaDataEntry makeEntry(Account account, Query query) {
    MetaDataEntry entry = new MetaDataEntry(account.getId(),
        query.getApplication(), FieldTypes.Query.ID, query.getId());
    if (query.getName() != null)
      entry.addField(FieldTypes.Query.NAME, query.getName());
    if (query.getDescription() != null)
      entry.addField(FieldTypes.Query.DESCRIPTION, query.getDescription());
    if (query.getServiceName() != null)
      entry.addField(FieldTypes.Query.SERVICE_NAME, query.getServiceName());
    if (query.isSetDatasets())
      entry.addField(FieldTypes.Query.DATASETS,
          ListToString(query.getDatasets()));
    return entry;
  }

  Query makeQuery(MetaDataEntry entry) {
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

  CompareStatus compare(Query query, MetaDataEntry existingEntry) {
    Query existing = makeQuery(existingEntry);
    CompareStatus status = CompareStatus.EQUAL;
    status = compareAlso(status, query.getId(), existing.getId());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, query.getName(), existing.getName());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(
        status, query.getDescription(), existing.getDescription());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, query.getServiceName(),
        existing.getServiceName());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, query.getDatasets(), existing.getDatasets());
    return status;
  }

  //-------------------------- Flow stuff ------------------------------------

  // When creating/updating a flow, you need to have id, name and application
  void validateFlow(Flow flow) throws MetadataServiceException {
    if (flow.getId() == null || flow.getId().isEmpty())
      throw new MetadataServiceException("Flow id is empty or null.");
    if (flow.getName() == null || flow.getName().isEmpty())
      throw new MetadataServiceException("Flow name is empty or null.");
    if(flow.getApplication() == null || flow.getApplication().isEmpty())
      throw new MetadataServiceException("Flow's app name is empty or null.");
  }

  MetaDataEntry makeEntry(Account account, Flow flow) {
    // Create a new metadata entry.
    MetaDataEntry entry = new MetaDataEntry(account.getId(),
        flow.getApplication(), FieldTypes.Flow.ID, flow.getId());
    entry.addField(FieldTypes.Flow.NAME, flow.getName());
    entry.addField(FieldTypes.Flow.STREAMS, ListToString(flow.getStreams()));
    entry.addField(FieldTypes.Flow.DATASETS, ListToString(flow.getDatasets()));
    return entry;
  }

  Flow makeFlow(MetaDataEntry entry) {
    return new Flow(entry.getId(), entry.getApplication(),
        entry.getTextField(FieldTypes.Flow.NAME),
        StringToList(entry.getTextField(FieldTypes.Flow.STREAMS)),
        StringToList(entry.getTextField(FieldTypes.Flow.DATASETS)));
  }

  CompareStatus compare(Flow flow, MetaDataEntry existingEntry) {
    Flow existing = makeFlow(existingEntry);
    CompareStatus status = CompareStatus.EQUAL;
    status = compareAlso(status, flow.getId(), existing.getId());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, flow.getName(), existing.getName());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, flow.getDatasets(), existing.getDatasets());
    if (status.equals(CompareStatus.DIFF)) return status;
    status = compareAlso(status, flow.getStreams(), existing.getStreams());
    return status;
  }

}
