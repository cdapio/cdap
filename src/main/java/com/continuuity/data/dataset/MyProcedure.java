package com.continuuity.data.dataset;

import com.continuuity.api.data.DataSetInstantiationException;
import com.continuuity.api.data.OperationException;
import com.continuuity.api.data.OperationResult;
import com.continuuity.api.data.set.IndexedTable;
import com.continuuity.api.data.set.KeyValueTable;
import com.continuuity.api.data.set.Table;

import java.util.Map;

/** sample procedure (query provider) to illustrate the use of data sets */
public class MyProcedure {

  KeyValueTable numbers;
  IndexedTable idxNumbers;

  static final byte[] phoneCol = { 'p', 'h', 'o', 'n', 'e' };
  static final byte[] nameCol = { 'n', 'a', 'm', 'e' };

  public void initialize(ExecutionContext cxt)
      throws DataSetInstantiationException, OperationException {

    numbers = cxt.getDataSet("phoneTable");
    idxNumbers = cxt.getDataSet("phoneIndex");
  }

  public void addToPhonebook(String name, String phone)
      throws OperationException {
    byte[] key = name.getBytes();
    byte[] phon = phone.getBytes();
    byte[][] cols = { phoneCol, nameCol };
    byte[][] vals = { phon, key };

    numbers.write(key, phon);
    idxNumbers.write(new Table.Write(key, cols, vals));
  }

  public String getPhoneNumber(String name) throws OperationException {
    byte[] bytes = this.numbers.read(name.getBytes());
    return bytes == null ? null : new String(bytes);
  }

  public String getNameByNumber(String number) throws OperationException {
    OperationResult<Map<byte[],byte[]>> result =
        this.idxNumbers.readBy(new Table.Read(number.getBytes(), nameCol));
    if (!result.isEmpty()) {
      byte[] bytes = result.getValue().get(nameCol);
      if (bytes != null) {
        return new String(bytes);
      }
    }
    return null;
  }
}
