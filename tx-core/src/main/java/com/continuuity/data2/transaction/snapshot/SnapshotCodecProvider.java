package com.continuuity.data2.transaction.snapshot;

import com.continuuity.common.conf.CConfiguration;
import com.continuuity.data2.transaction.TxConstants;
import com.continuuity.data2.transaction.persist.TransactionSnapshot;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.SortedMap;
import javax.annotation.Nonnull;

/**
 * Maintains the codecs for all known versions of the transaction snapshot encoding.
 */
public class SnapshotCodecProvider implements SnapshotCodec {

  private static final Logger LOG = LoggerFactory.getLogger(SnapshotCodecProvider.class);

  private final SortedMap<Integer, SnapshotCodec> codecs = Maps.newTreeMap();

  @Inject
  public SnapshotCodecProvider(CConfiguration configuration) {
    initialize(configuration);
  }

  /**
   * Register all codec specified in the configuration with this provider.
   * There can only be one codec for a given version.
   */
  private void initialize(CConfiguration configuration) {
    Class<?>[] codecClasses = configuration.getClasses(TxConstants.Persist.CFG_TX_SNAPHOT_CODEC_CLASSES);
    if (codecClasses == null || codecClasses.length == 0) {
      codecClasses = TxConstants.Persist.DEFAULT_TX_SNAPHOT_CODEC_CLASSES;
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
  private SnapshotCodec getCodecForVersion(int version) {
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

  @Override
  public int getVersion() {
    return getCurrentCodec().getVersion();
  }

  @Override
  public TransactionSnapshot decode(InputStream in) {
    // Picking at version to create appropriate codec
    BinaryDecoder decoder = new BinaryDecoder(in);
    int persistedVersion;
    try {
      persistedVersion = decoder.readInt();
    } catch (IOException e) {
      LOG.error("Unable to read transaction state version: ", e);
      throw Throwables.propagate(e);
    }
    SnapshotCodec codec = getCodecForVersion(persistedVersion);
    return codec.decode(in);
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
