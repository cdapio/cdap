/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tephra.snapshot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.hadoop.conf.Configuration;
import org.apache.tephra.TxConstants;
import org.apache.tephra.persist.TransactionSnapshot;
import org.apache.tephra.persist.TransactionVisibilityState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.SortedMap;
import javax.annotation.Nonnull;

/**
 * Maintains the codecs for all known versions of the transaction snapshot encoding.
 */
public class SnapshotCodecProvider implements SnapshotCodec {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotCodecProvider.class);

  private final SortedMap<Integer, SnapshotCodec> codecs = Maps.newTreeMap();

  @Inject
  public SnapshotCodecProvider(Configuration configuration) {
    initialize(configuration);
  }

  /**
   * Register all codec specified in the configuration with this provider.
   * There can only be one codec for a given version.
   */
  private void initialize(Configuration configuration) {
    String[] codecClassNames = configuration.getTrimmedStrings(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);
    List<Class> codecClasses = Lists.newArrayList();
    if (codecClassNames != null) {
      for (String clsName : codecClassNames) {
        try {
          codecClasses.add(Class.forName(clsName));
        } catch (ClassNotFoundException cnfe) {
          LOG.warn("Unable to load class configured for " + TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES
              + ": " + clsName, cnfe);
        }
      }
    }

    if (codecClasses.size() == 0) {
      codecClasses.addAll(Arrays.asList(TxConstants.Persist.DEFAULT_TX_SNAPHOT_CODEC_CLASSES));
    }
    for (Class<?> codecClass : codecClasses) {
      try {
        SnapshotCodec codec = (SnapshotCodec) (codecClass.newInstance());
        codecs.put(codec.getVersion(), codec);
        LOG.debug("Using snapshot codec {} for snapshots of version {}", codecClass.getName(), codec.getVersion());
      } catch (Exception e) {
        LOG.warn("Error instantiating snapshot codec {}. Skipping.", codecClass.getName(), e);
      }
    }
  }

  /**
   * Retrieve the codec for a particular version of the encoding.
   * @param version the version of interest
   * @return the corresponding codec
   * @throws java.lang.IllegalArgumentException if the version is not known
   */
  @Nonnull
  @VisibleForTesting
  SnapshotCodec getCodecForVersion(int version) {
    SnapshotCodec codec = codecs.get(version);
    if (codec == null) {
      throw new IllegalArgumentException(String.format("Version %d of snapshot encoding is not supported", version));
    }
    return codec;
  }

  /**
   * Retrieve the current snapshot codec, that is, the codec with the highest known version.
   * @return the current codec
   * @throws java.lang.IllegalStateException if no codecs are registered
   */
  private SnapshotCodec getCurrentCodec() {
    if (codecs.isEmpty()) {
      throw new IllegalStateException(String.format("No codecs are registered."));
    }
    return codecs.get(codecs.lastKey());
  }

  // Return the appropriate codec for the version in InputStream
  private SnapshotCodec getCodec(InputStream in) {
    BinaryDecoder decoder = new BinaryDecoder(in);
    int persistedVersion;
    try {
      persistedVersion = decoder.readInt();
    } catch (IOException e) {
      LOG.error("Unable to read transaction state version: ", e);
      throw Throwables.propagate(e);
    }
    return getCodecForVersion(persistedVersion);
  }

  @Override
  public int getVersion() {
    return getCurrentCodec().getVersion();
  }

  @Override
  public TransactionSnapshot decode(InputStream in) {
    return getCodec(in).decode(in);
  }

  @Override
  public TransactionVisibilityState decodeTransactionVisibilityState(InputStream in) {
    return getCodec(in).decodeTransactionVisibilityState(in);
  }

  @Override
  public void encode(OutputStream out, TransactionSnapshot snapshot) {
    SnapshotCodec codec = getCurrentCodec();
    try {
      new BinaryEncoder(out).writeInt(codec.getVersion());
    } catch (IOException e) {
      LOG.error("Unable to write transaction state version: ", e);
      throw Throwables.propagate(e);
    }
    codec.encode(out, snapshot);
  }

}
