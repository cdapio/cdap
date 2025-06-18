package io.cdap.cdap.master.export_import;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import javax.annotation.Nullable;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;

/**
 * Manages collecting and writing success and failure reports for the export job
 * by streaming them directly to GCS to avoid high memory usage.
 */
/**
 * A reusable, streaming report generator for import or export jobs.
 * It writes success and failure records to CSV files in GCS to avoid high memory usage.
 */
class JobReport {
  private static final Logger LOG = LoggerFactory.getLogger(JobReport.class);

  /**
   * Defines the type of job to configure report headers and filenames correctly.
   */
  public enum JobType {
    IMPORT, EXPORT
  }

  private final JobType jobType;
  private final String[] successHeader;
  private final String[] failureHeader = {"Timestamp", "ArtifactType", "ArtifactName", "Namespace", "Status", "ErrorMessage"};

  @Nullable private Writer successWriter;
  @Nullable private Writer failureWriter;
  private boolean isOpen = false;

  public JobReport(JobType jobType) {
    this.jobType = jobType;
    // Configure the success header based on the job type
    if (this.jobType == JobType.IMPORT) {
      this.successHeader = new String[]{"Timestamp", "ArtifactType", "ArtifactName", "Namespace", "Status", "SourcePath"};
    } else {
      this.successHeader = new String[]{"Timestamp", "ArtifactType", "ArtifactName", "Namespace", "Status", "DestinationPath"};
    }
  }

  public synchronized void open(LocationFactory locationFactory, String backupPath) throws IOException {
    if (isOpen) {
      LOG.warn("Report writers are already open. Ignoring request.");
      return;
    }
    String filePrefix = jobType == JobType.IMPORT ? "_IMPORT" : "_EXPORT";
    LOG.info("Opening {} report writers...", jobType.toString().toLowerCase());
    Location baseLocation = locationFactory.create(backupPath);
    if (!baseLocation.exists()) {
      baseLocation.mkdirs();
    }

    try {
      Location successReportFile = baseLocation.append(filePrefix + "_SUCCESS_REPORT.csv");
      this.successWriter = new OutputStreamWriter(successReportFile.getOutputStream(), StandardCharsets.UTF_8);
      this.successWriter.write(convertToCsvRow(successHeader));

      Location failureReportFile = baseLocation.append(filePrefix + "_FAILURE_REPORT.csv");
      this.failureWriter = new OutputStreamWriter(failureReportFile.getOutputStream(), StandardCharsets.UTF_8);
      this.failureWriter.write(convertToCsvRow(failureHeader));

      this.isOpen = true;
      LOG.info("{} report writers opened successfully.", jobType.toString());
    } catch (IOException e) {
      LOG.error("Failed to open GCS streams for reporting. Reports will not be generated.", e);
      close();
      throw e;
    }
  }

  public boolean isOpen() {
    return this.isOpen;
  }

  public synchronized void addSuccess(String artifactType, String artifactName, String namespace, String path) {
    if (!isOpen || successWriter == null) {
      LOG.error("Report writer is not open. Cannot add success record for {}/{}", namespace, artifactName);
      return;
    }
    try {
      String[] record = {Instant.now().toString(), artifactType, artifactName, namespace, "SUCCESS", path};
      successWriter.write(convertToCsvRow(record));
    } catch (IOException e) {
      LOG.error("Failed to write success record for {}/{} to GCS", namespace, artifactName, e);
    }
  }

  public synchronized void addFailure(String artifactType, String artifactName, String namespace, String errorMessage) {
    if (!isOpen || failureWriter == null) {
      LOG.error("Report writer is not open. Cannot add failure record for {}/{}", namespace, artifactName);
      return;
    }
    try {
      String cleanErrorMessage = (errorMessage == null) ? "null" : errorMessage.replace("\n", " | ");
      String[] record = {Instant.now().toString(), artifactType, artifactName, namespace, "FAILURE", cleanErrorMessage};
      failureWriter.write(convertToCsvRow(record));
    } catch (IOException e) {
      LOG.error("Failed to write failure record for {}/{} to GCS", namespace, artifactName, e);
    }
  }

  public synchronized void close() {
    if (!isOpen) {
      return;
    }
    LOG.info("Closing {} report writers...", jobType.toString().toLowerCase());
    try {
      if (successWriter != null) {
        successWriter.flush();
        successWriter.close();
      }
    } catch (IOException e) {
      LOG.error("Failed to close success report writer.", e);
    }
    try {
      if (failureWriter != null) {
        failureWriter.flush();
        failureWriter.close();
      }
    } catch (IOException e) {
      LOG.error("Failed to close failure report writer.", e);
    }
    isOpen = false;
    LOG.info("{} report writers closed.", jobType.toString());
  }

  private String convertToCsvRow(String[] data) {
    return Stream.of(data)
        .map(this::escapeCsvField)
        .collect(Collectors.joining(",")) + "\n";
  }

  private String escapeCsvField(String data) {
    if (data == null) {
      return "";
    }
    if (data.contains(",") || data.contains("\"") || data.contains("\n")) {
      return "\"" + data.replace("\"", "\"\"") + "\"";
    }
    return data;
  }
}
