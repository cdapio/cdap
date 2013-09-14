package com.continuuity.gateway;

import com.continuuity.api.data.OperationException;
import com.continuuity.data.metadata.MetaDataEntry;
import com.continuuity.data.metadata.MetaDataStore;
import com.continuuity.data.operation.OperationContext;
import com.continuuity.metadata.MetadataService;
import com.continuuity.metadata.thrift.Account;
import com.continuuity.metadata.thrift.Stream;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * this is for testing and returns exists=true for all streams that do not
 * start with "x".
 */
public class DummyMDS extends MetadataService {
  DummyMDS() {
    super(new DummyStore());
  }

  boolean allowAll = false;

  public void allowAll() {
    this.allowAll = true;
  }

  @Override
  public Stream getStream(Account account, Stream stream) {
    if (!allowAll && stream.getId().startsWith("x")) {
      stream.setExists(false);
    }
    return stream;
  }
}

class DummyStore implements MetaDataStore {
  @Override
  public void add(OperationContext context, MetaDataEntry entry) throws OperationException {
    // do nothing
  }

  @Override
  public void add(OperationContext context, MetaDataEntry entry, boolean resolve) throws OperationException {
    // do nothing
  }

  @Override
  public void update(OperationContext context, MetaDataEntry entry) throws OperationException {
    // do nothing
  }

  @Override
  public void update(OperationContext context, MetaDataEntry entry, boolean resolve) throws OperationException {
    // do nothing
  }

  @Override
  public void swap(OperationContext context, MetaDataEntry expected, MetaDataEntry entry)
    throws OperationException {
    // do nothing
  }

  @Override
  public void updateField(OperationContext context, String account, String application, String type, String id,
                          String field, String newValue, int retryAttempts)
    throws OperationException {
    // do nothing
  }

  @Override
  public void updateField(OperationContext context, String account, String application, String type, String id,
                          String field, byte[] newValue, int retryAttempts)
    throws OperationException {
    // do nothing
  }

  @Override
  public void swapField(OperationContext context, String account, String application, String type, String id,
                        String field, String oldValue, String newValue, int retryAttempts)
    throws OperationException {
    // do nothing
  }

  @Override
  public void swapField(OperationContext context, String account, String application, String type, String id,
                        String field, byte[] oldValue, byte[] newValue, int retryAttempts)
    throws OperationException {
    // do nothing
  }

  @Override
  public void delete(OperationContext context, String account, String application, String type, String id)
    throws OperationException {
    // do nothing
  }

  @Override
  public MetaDataEntry get(OperationContext context, String account, String application, String type, String id)
    throws OperationException {
    return null; // dummy
  }

  @Override
  public List<MetaDataEntry> list(OperationContext context, String account, String application, String type,
                                  Map<String, String> fields)
    throws OperationException {
    return null; // dummy
  }

  @Override
  public void clear(OperationContext context, String account, String application) throws OperationException {
    // do nothing
  }

  @Override
  public Collection<String> listAccounts(OperationContext context) throws OperationException {
    return null; // dummy
  }
}
