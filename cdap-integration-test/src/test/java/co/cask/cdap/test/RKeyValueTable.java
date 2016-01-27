/*
 * Copyright © 2015 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.test;

import co.cask.cdap.api.data.batch.RecordScanner;
import co.cask.cdap.api.data.batch.Split;
import co.cask.cdap.api.data.batch.SplitReader;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.client.DatasetClient;
import co.cask.cdap.common.BadRequestException;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.common.UnauthorizedException;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.dataset.DatasetMethodArgument;
import co.cask.cdap.proto.dataset.DatasetMethodRequest;
import co.cask.cdap.proto.dataset.DatasetMethodResponse;
import com.google.common.base.Throwables;
import com.google.gson.Gson;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class RKeyValueTable extends KeyValueTable {

  private static final Gson GSON = new Gson();
  private final Id.DatasetInstance instance;
  private final DatasetClient client;

  public RKeyValueTable(Id.DatasetInstance instance, DatasetClient client) {
    super(null, null);
    this.instance = instance;
    this.client = client;
  }

  @SuppressWarnings("unchecked")
  private <T> T execute(String method, Class<T> returnType, Object[] arguments) {
    List<DatasetMethodArgument> methodArguments = new ArrayList<>();
    for (Object argument : arguments) {
      methodArguments.add(new DatasetMethodArgument(argument.getClass().getName(), GSON.toJsonTree(argument)));
    }

    try {
      DatasetMethodResponse<T> response = client.execute(
        instance, new DatasetMethodRequest(method, returnType.getName(), methodArguments), returnType);
      return response.getResponse();
    } catch (IOException | UnauthorizedException | BadRequestException | NotFoundException e) {
      throw Throwables.propagate(e);
    }
  }

  @Nullable
  @Override
  public byte[] read(String key) {
    return execute("read", byte[].class, new Object[] { key });
  }

  @Nullable
  @Override
  public String readString(String key) {
    return execute("readString", String.class, new Object[] { key });
  }

  @Nullable
  @Override
  public byte[] read(byte[] key) {
    return execute("read", byte[].class, new Object[] { key });
  }

  @Override
  public Map<byte[], byte[]> readAll(byte[][] keys) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long incrementAndGet(byte[] key, long value) {
    return execute("incrementAndGet", long.class, new Object[] { key, value });
  }

  @Override
  public void write(byte[] key, byte[] value) {
    execute("write", void.class, new Object[] { key, value });
  }

  @Override
  public void write(String key, String value) {
    execute("write", void.class, new Object[] { key, value });
  }

  @Override
  public void write(String key, byte[] value) {
    execute("write", void.class, new Object[] { key, value });
  }

  @Override
  public void increment(byte[] key, long amount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(byte[] key) {
    execute("delete", void.class, new Object[] { key });
  }

  @Override
  public boolean compareAndSwap(byte[] key, byte[] oldValue, byte[] newValue) {
    return execute("compareAndSwap", boolean.class, new Object[] { key, oldValue, newValue });
  }

  @Override
  public Type getRecordType() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Split> getSplits() {
    throw new UnsupportedOperationException();
  }

  @Override
  public RecordScanner<KeyValue<byte[], byte[]>> createSplitRecordScanner(Split split) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Split> getSplits(int numSplits, byte[] start, byte[] stop) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SplitReader<byte[], byte[]> createSplitReader(Split split) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(KeyValue<byte[], byte[]> keyValue) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CloseableIterator<KeyValue<byte[], byte[]>> scan(byte[] startRow, byte[] stopRow) {
    throw new UnsupportedOperationException();
  }
}