package com.continuuity.api.data;

import java.util.List;
import java.util.Map;

public interface MetaDataStore {

  /** adds a new entry, throws if an entry with that name already exists */
  public void add(OperationContext context,
                  MetaDataEntry entry) throws
      MetaDataException;

  /** updates an entry, throws if an entry with that name does not exist */
  public void update(OperationContext conetxt,
                     MetaDataEntry entry) throws
      MetaDataException;

  /** delete by name & type */
  public void delete(OperationContext conetxt,
                     String account, String application,
                     String type, String name)
      throws MetaDataException;

  /** get by name & type */
  public MetaDataEntry get(OperationContext conetxt,
                           String account, String application,
                           String type, String name)
      throws MetaDataException;

  /** list all entries of a given type */
  public List<MetaDataEntry> list(OperationContext conetxt,
                                  String account, String application,
                                  String type, Map<String, String> fields)
      throws MetaDataException;

  /** delete all entries for an account or application */
  public void clear(OperationContext conetxt,
                    String account, String application)
      throws MetaDataException;
}
