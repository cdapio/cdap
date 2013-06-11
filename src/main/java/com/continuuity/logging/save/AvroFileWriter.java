package com.continuuity.logging.save;

import com.continuuity.logging.kafka.KafkaLogEvent;
import com.google.common.collect.Maps;
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
import java.util.Map;

/**
 * Helper class that manages writing of KafkaLogEvent to Avro files. The events are written into appropriate files
 * based on the LoggingContext of the event. The files are also rotated based on size. This class is not thread-safe.
 */
public class AvroFileWriter implements Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(AvroFileWriter.class);

  private final FileSystem fileSystem;
  private final Schema schema;
  private final int syncIntervalBytes;
  private final String pathRoot;
  private final Map<String, AvroFile> fileMap;
  private final long maxFileSize;

  /**
   * Constructs an AvroFileWriter object.
   * @param fileSystem fileSystem where the Avro files are to be created.
   * @param pathRoot Root path for the files to be created.
   * @param schema schema of the Avro data to be written.
   * @param maxFileSize Avro files greater than maxFileSize will get rotated.
   * @param syncIntervalBytes the approximate number of uncompressed bytes to write in each block.
   */
  public AvroFileWriter(FileSystem fileSystem, String pathRoot, Schema schema, long maxFileSize,
                        int syncIntervalBytes) {
    this.fileSystem = fileSystem;
    this.schema = schema;
    this.syncIntervalBytes = syncIntervalBytes;
    this.pathRoot = pathRoot;
    this.fileMap = Maps.newConcurrentMap();
    this.maxFileSize = maxFileSize;
  }

  /**
   * Appends a log event to an appropriate Avro file based on LoggingContext. If the log event does not contain
   * LoggingContext then the event will be dropped.
   * @param event Log event
   * @throws IOException
   */
  public void append(KafkaLogEvent event) throws IOException {
    String pathFragment = event.getLoggingContext().getLogPathFragment();
    if (pathFragment == null) {
      LOG.debug(String.format("path fragment is null for event %s. Skipping it.", event));
      return;
    }

    AvroFile avroFile = getAvroFile(pathFragment);
    avroFile.getDataFileWriter().append(event.getGenericRecord());

    rotateFile(avroFile, pathFragment);
  }

  @Override
  public void close() throws IOException {
    // Close all files
    for (Map.Entry<String, AvroFile> entry : fileMap.entrySet()) {
      entry.getValue().close();
    }
    fileMap.clear();
  }

  private AvroFile getAvroFile(String pathFragment) throws IOException {
    AvroFile avroFile = fileMap.get(pathFragment);
    if (avroFile == null) {
      return createAvroFile(pathFragment);
    }
    return avroFile;
  }

  private AvroFile createAvroFile(String pathFragment) throws IOException {
    Path path = createPath(pathFragment);
    LOG.info(String.format("Creating Avro file %s", path.toUri()));
    AvroFile avroFile = new AvroFile(path);
    fileMap.put(pathFragment, avroFile);
    return avroFile;
  }

  private Path createPath(String pathFragment) {
    String date = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
    return new Path(String.format("%s/%s/%s/%s.avro", pathRoot, pathFragment, date, System.currentTimeMillis()));
  }

  private void rotateFile(AvroFile avroFile, String pathFragment) throws IOException {
    if (avroFile.getOutputStream().getPos() > maxFileSize) {
      avroFile.close();
      createAvroFile(pathFragment);
    }
  }

  /**
   * Represents an Avro file.
   */
  class AvroFile implements Closeable {
    private final FSDataOutputStream outputStream;
    private final DataFileWriter<GenericRecord> dataFileWriter;

    public AvroFile(Path path) throws IOException {
      this.outputStream = fileSystem.create(path, false);
      this.dataFileWriter = new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(schema));
      this.dataFileWriter.create(schema, this.outputStream);
      this.dataFileWriter.setSyncInterval(syncIntervalBytes);
    }

    public FSDataOutputStream getOutputStream() {
      return outputStream;
    }

    public DataFileWriter<GenericRecord> getDataFileWriter() {
      return dataFileWriter;
    }

    @Override
    public void close() throws IOException {
      dataFileWriter.close();
      outputStream.close();
    }
  }
}
