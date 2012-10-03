package com.continuuity.api.data;

import java.util.List;
import java.util.Map;

public interface MetaDataStore {

  /** adds a new entry, throws if an entry with that name already exists */
  public void add(OperationContext context,
                  MetaDataEntry entry) throws OperationException;

  /** updates an entry, throws if an entry with that name does not exist */
  public void update(OperationContext context,
                     MetaDataEntry entry) throws OperationException;

  /** delete by name & type */
  public void delete(OperationContext context,
                     String account, String application,
                     String type, String name)
      throws OperationException;

  /** get by name & type */
  public MetaDataEntry get(OperationContext context,
                           String account, String application,
                           String type, String name)
      throws OperationException;

  /** list all entries of a given type */
  public List<MetaDataEntry> list(OperationContext context,
                                  String account, String application,
                                  String type, Map<String, String> fields)
      throws OperationException;

  /** delete all entries for an account or application */
  public void clear(OperationContext context,
                    String account, String application)
      throws OperationException;
}
