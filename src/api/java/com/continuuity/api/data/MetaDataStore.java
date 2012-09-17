package com.continuuity.api.data;

import java.util.List;
import java.util.Map;

public interface MetaDataStore {

  /** adds a new entry, throws if an entry with that name already exists */
  public void add(MetaDataEntry entry) throws MetaDataException;

  /** updates an entry, throws if an entry with that name does not exist */
  public void update(MetaDataEntry entry) throws MetaDataException;

  /** get by name */
  public MetaDataEntry get(String name) throws MetaDataException;

  /** list all entries of a given type */
  public List<MetaDataEntry> list(String type);

  /** find all entries that match some given fields */
  public List<MetaDataEntry> find(Map<String, String> fields);
}
