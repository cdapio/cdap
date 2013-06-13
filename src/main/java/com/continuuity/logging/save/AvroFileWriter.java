package com.continuuity.logging.save;

import com.continuuity.api.data.OperationException;
import com.continuuity.common.logging.LoggingContext;
import com.continuuity.logging.kafka.KafkaLogEvent;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.continuuity.logging.save.CheckpointManager.CheckpointInfo;

/**
 * Helper class that manages writing of KafkaLogEvent to Avro files. The events are written into appropriate files
 * based on the LoggingContext of the event. The files are also rotated based on size. This class is not thread-safe.
 */
public final class AvroFileWriter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileWriter.class);

  private final CheckpointManager checkpointManager;
  private final FileManager fileManager;
  private final FileSystem fileSystem;
  private final Schema schema;
  private final int syncIntervalBytes;
  private final String pathRoot;
  private final Map<String, AvroFile> fileMap;
  private final long maxFileSize;
  private final long checkpointIntervalMs;
  private final long inactiveIntervalMs;

  private long lastCheckpointTime = System.currentTimeMillis();

  /**
   * Constructs an AvroFileWriter object.
   * @param checkpointManager used to store checkpoint meta data.
   * @param fileManager used to store file meta data.
   * @param fileSystem fileSystem where the Avro files are to be created.
   * @param pathRoot Root path for the files to be created.
   * @param schema schema of the Avro data to be written.
   * @param maxFileSize Avro files greater than maxFileSize will get rotated.
   * @param syncIntervalBytes the approximate number of uncompressed bytes to write in each block.
   * @param checkpointIntervalMs interval to save checkpoint.
   * @param inactiveIntervalMs files that have no data written for more than inactiveIntervalMs will be closed.
   */
  public AvroFileWriter(CheckpointManager checkpointManager, FileManager fileManager, FileSystem fileSystem,
                        String pathRoot, Schema schema,
                        long maxFileSize, int syncIntervalBytes, long checkpointIntervalMs, long inactiveIntervalMs) {
    this.checkpointManager = checkpointManager;
    this.fileManager = fileManager;
    this.fileSystem = fileSystem;
    this.schema = schema;
    this.syncIntervalBytes = syncIntervalBytes;
    this.pathRoot = pathRoot;
    this.fileMap = Maps.newConcurrentMap();
    this.maxFileSize = maxFileSize;
    this.checkpointIntervalMs = checkpointIntervalMs;
    this.inactiveIntervalMs = inactiveIntervalMs;
  }

  /**
   * Appends a log event to an appropriate Avro file based on LoggingContext. If the log event does not contain
   * LoggingContext then the event will be dropped.
   * @param event Log event
   * @throws IOException
   */
  public void append(KafkaLogEvent event) throws IOException, OperationException {
    String pathFragment = event.getLoggingContext().getLogPathFragment();
    if (pathFragment == null) {
      LOG.debug(String.format("path fragment is null for event %s. Skipping it.", event));
      return;
    }

    AvroFile avroFile = getAvroFile(event.getLoggingContext());
    avroFile.append(event);

    checkPoint(false);
    rotateFile(avroFile, event.getLoggingContext());
  }

  @Override
  public void close() throws IOException {
    // Close all files
    LOG.info("Closing all files");
    for (Map.Entry<String, AvroFile> entry : fileMap.entrySet()) {
      entry.getValue().close();
    }
    fileMap.clear();
    try {
      checkPoint(true);
    } catch (OperationException e) {
      LOG.error("Caught exception while closing", e);
    }
  }

  private AvroFile getAvroFile(LoggingContext loggingContext) throws IOException, OperationException {
    AvroFile avroFile = fileMap.get(loggingContext.getLogPathFragment());
    if (avroFile == null) {
      avroFile = createAvroFile(loggingContext);
      fileMap.put(loggingContext.getLogPathFragment(), avroFile);
    }
    return avroFile;
  }

  private AvroFile createAvroFile(LoggingContext loggingContext) throws IOException, OperationException {
    long currentTs = System.currentTimeMillis();
    Path path = createPath(loggingContext.getLogPathFragment(), currentTs);
    LOG.info(String.format("Creating Avro file %s", path.toUri()));
    AvroFile avroFile = new AvroFile(path);
    fileManager.write(loggingContext, currentTs, path.toUri().toString());
    return avroFile;
  }

  private Path createPath(String pathFragment, long currentTs) {
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    return new Path(String.format("%s/%s/%s/%s.avro", pathRoot, pathFragment, date, currentTs));
  }

  private void rotateFile(AvroFile avroFile, LoggingContext loggingContext) throws IOException, OperationException {
    if (avroFile.getPos() > maxFileSize) {
      avroFile.close();
      createAvroFile(loggingContext);
      checkPoint(true);
    }
  }

  private void checkPoint(boolean force) throws IOException, OperationException {
    long currentTs = System.currentTimeMillis();
    if (!force && currentTs - lastCheckpointTime < checkpointIntervalMs) {
      return;
    }

    long checkpointOffset = Long.MAX_VALUE;
    Set<String> files = Sets.newHashSetWithExpectedSize(fileMap.size());
    for (Iterator<Map.Entry<String, AvroFile>> it = fileMap.entrySet().iterator(); it.hasNext();) {
      AvroFile avroFile = it.next().getValue();
      avroFile.flush();

      // Close inactive files
      if (currentTs - avroFile.getLastWriteTs() > inactiveIntervalMs) {
        avroFile.close();
        it.remove();
      }

      files.add(avroFile.getPath().toUri().toString());
      if (checkpointOffset > avroFile.getMaxOffsetSeen()) {
        checkpointOffset = avroFile.getMaxOffsetSeen();
      }
    }

    if (checkpointOffset != Long.MAX_VALUE) {
      checkpointManager.saveCheckpoint(new CheckpointInfo(checkpointOffset, files));
    }
    lastCheckpointTime = currentTs;
  }

  /**
   * Represents an Avro file.
   */
  public class AvroFile implements Closeable {
    private final Path path;
    private final FSDataOutputStream outputStream;
    private final DataFileWriter<GenericRecord> dataFileWriter;
    private long maxOffsetSeen = -1;
    private long lastWriteTs;

    public AvroFile(Path path) throws IOException {
      this.path = path;
      this.outputStream = fileSystem.create(path, false);
      this.dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
      this.dataFileWriter.create(schema, this.outputStream);
      this.dataFileWriter.setSyncInterval(syncIntervalBytes);
      this.lastWriteTs = System.currentTimeMillis();
    }

    public Path getPath() {
      return path;
    }

    public void append(KafkaLogEvent event) throws IOException {
      dataFileWriter.append(event.getGenericRecord());
      if (event.getOffset() > maxOffsetSeen) {
        maxOffsetSeen = event.getOffset();
      }
      lastWriteTs = System.currentTimeMillis();
    }

    public long getPos() throws IOException {
      return outputStream.getPos();
    }

    public long getMaxOffsetSeen() {
      return maxOffsetSeen;
    }

    public long getLastWriteTs() {
      return lastWriteTs;
    }

    public void flush() throws IOException {
      dataFileWriter.flush();
      outputStream.flush();
    }

    @Override
    public void close() throws IOException {
      dataFileWriter.close();
      outputStream.close();
    }
  }
}
