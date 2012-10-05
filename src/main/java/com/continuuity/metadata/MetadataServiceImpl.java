package com.continuuity.metadata;

import com.continuuity.api.data.MetaDataEntry;
import com.continuuity.api.data.MetaDataStore;
import com.continuuity.api.data.OperationContext;
import com.continuuity.metadata.stubs.*;
import org.apache.thrift.TException;

import java.util.List;

/**
 *
 */
public class MetadataServiceImpl implements MetadataService.Iface {

  public MetadataServiceImpl(MetaDataStore mds) {
  }

  /**
   * Creates a stream.
   *
   * @param stream
   * @return true if successful; false otherwise
   * @throws com.continuuity.metadata.stubs.MetadataServiceException
   *          thrown when there is issue with creating
   *          stream.
   */
  @Override
  public boolean createStream(Account account, Stream stream)
    throws MetadataServiceException, TException {

    OperationContext context = new OperationContext(account.getId());

    MetaDataEntry entry = new MetaDataEntry(
      account.getId(), null, FieldTypes.Stream.TYPE, stream.getId()
    );
    entry.addField(FieldTypes.Stream.NAME, stream.getName());
    entry.addField(FieldTypes.Stream.DESCRIPTION, stream.getDescription());
    entry.addField(FieldTypes.Stream.CREATE_DATE,
                   String.format("%d", System.currentTimeMillis()));
    return false;
  }

  /**
   * Deletes a stream.
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
    return false;
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
    return null;
  }

  /**
   * Creates a dataset.
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
    return false;
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
    return false;
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
    return null;
  }
}
