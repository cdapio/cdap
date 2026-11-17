/*
 * Copyright © 2022 Cask Data, Inc.
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

package io.cdap.cdap.support.services;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.cdap.cdap.common.BadRequestException;
import io.cdap.cdap.common.NamespaceNotFoundException;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.namespace.RemoteNamespaceQueryClient;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.NamespaceMeta;
import io.cdap.cdap.proto.id.EntityId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.support.SupportBundleTaskConfiguration;
import io.cdap.cdap.support.job.SupportBundleJob;
import io.cdap.cdap.support.lib.SupportBundleFileNames;
import io.cdap.cdap.support.lib.SupportBundleOperationStatus;
import io.cdap.cdap.support.lib.SupportBundleRequestFileList;
import io.cdap.cdap.support.status.CollectionState;
import io.cdap.cdap.support.status.SupportBundleConfiguration;
import io.cdap.cdap.support.status.SupportBundleStatus;
import io.cdap.cdap.support.status.SupportBundleTaskStatus;
import io.cdap.cdap.support.task.factory.SupportBundleTaskFactory;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.FileVisitResult;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Support bundle service to generate base path, uuid and trigger the job to execute tasks.
 */
public class SupportBundleGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleGenerator.class);
  private static final Gson GSON = new Gson();
  private final Set<SupportBundleTaskFactory> taskFactories;
  private final CConfiguration cConf;
  private final RemoteNamespaceQueryClient namespaceQueryClient;
  private final String localDir;

  @Inject
  SupportBundleGenerator(CConfiguration cConf, RemoteNamespaceQueryClient namespaceQueryClient,
      @Named(Constants.SupportBundle.TASK_FACTORY) Set<SupportBundleTaskFactory> taskFactories) {
    this.cConf = cConf;
    this.namespaceQueryClient = namespaceQueryClient;
    this.localDir = cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR);
    this.taskFactories = taskFactories;
  }

  /**
   * Generates support bundle
   */
  public String generate(SupportBundleConfiguration config, ExecutorService executorService)
      throws Exception {
    validateConfiguration(config);
    NamespaceId namespace = Optional.ofNullable(config.getNamespace()).map(NamespaceId::new)
        .orElse(null);
    List<NamespaceId> namespaces = new ArrayList<>();

    if (namespace == null) {
      namespaces.addAll(
          namespaceQueryClient.list().stream().map(NamespaceMeta::getNamespaceId)
              .collect(Collectors.toList()));
    } else {
      namespaces.add(validNamespace(namespace));
    }
    // Puts all the files under the uuid path
    File baseDirectory = new File(localDir);
    DirUtils.mkdirs(baseDirectory);
    String uuid = UUID.randomUUID().toString();
    File uuidPath = new File(localDir, uuid);

    deleteOldFoldersIfExceedLimit(baseDirectory);
    DirUtils.mkdirs(uuidPath);

    SupportBundleStatus supportBundleStatus = SupportBundleStatus.builder()
        .setBundleId(uuid)
        .setStartTimestamp(System.currentTimeMillis())
        .setStatus(CollectionState.IN_PROGRESS)
        .setParameters(config)
        .build();
    addToStatus(supportBundleStatus, uuidPath.getPath());

    SupportBundleJob supportBundleJob =
        new SupportBundleJob(taskFactories, executorService, cConf, supportBundleStatus);
    SupportBundleTaskConfiguration supportBundleTaskConfiguration =
        new SupportBundleTaskConfiguration(config, uuid, uuidPath, namespaces, supportBundleJob);

    try {
      executorService.execute(
          () -> supportBundleJob.generateBundle(supportBundleTaskConfiguration));
    } catch (Exception e) {
      LOG.error("Failed to finish execute tasks", e);
      SupportBundleStatus failedBundleStatus = SupportBundleStatus.builder(supportBundleStatus)
          .setStatus(CollectionState.FAILED)
          .setFinishTimestamp(System.currentTimeMillis())
          .setStatusDetails(e.getMessage())
          .build();
      addToStatus(failedBundleStatus, uuidPath.getPath());
    }

    return uuid;
  }

  /**
   * Check whether the prev bundle is still processing or not
   */
  @Nullable
  public String getInProgressBundle() throws IOException {
    File latestDirectory = getLatestFolder();
    if (latestDirectory == null) {
      return null;
    }
    SupportBundleStatus supportBundleStatus = getBundleStatus(latestDirectory);
    if (supportBundleStatus != null
        && supportBundleStatus.getStatus() == CollectionState.IN_PROGRESS) {
      return supportBundleStatus.getBundleId();
    }
    return null;
  }

  /**
   * Gets single support bundle status with uuid
   */
  @VisibleForTesting
  SupportBundleStatus getBundleStatus(File uuidFile) throws IllegalArgumentException, IOException {
    File statusFile = new File(uuidFile, SupportBundleFileNames.STATUS_FILE_NAME);
    if (!statusFile.exists()) {
      throw new IllegalArgumentException("Failed to find this status file");
    }
    return readStatusJson(statusFile);
  }

  /**
   * Deletes old folders after certain number of folders exist
   */
  @VisibleForTesting
  public void deleteOldFoldersIfExceedLimit(File baseDirectory) throws IOException {
    int fileCount = DirUtils.list(baseDirectory).size();
    // We want to keep consistent number of bundle to provide to customer
    int folderMaxNumber = cConf.getInt(Constants.SupportBundle.MAX_FOLDER_SIZE);
    if (fileCount >= folderMaxNumber) {
      File oldFilesDirectory = getOldestFolder(baseDirectory);
      DirUtils.deleteDirectoryContents(oldFilesDirectory);
    }
  }

  /**
   * Deletes select folder
   */
  public void deleteBundle(String uuid) throws IOException, NotFoundException {
    File uuidFile = getUUIDFile(uuid);
    if (!uuidFile.exists()) {
      throw new NotFoundException(String.format("No such uuid '%s' in Support Bundle.", uuid));
    }
    DirUtils.deleteDirectoryContents(uuidFile);
  }

  /**
   * Get single support bundle overall status
   */
  public SupportBundleOperationStatus getBundle(String uuid) throws IOException, NotFoundException {
    File uuidFile = getUUIDFile(uuid);
    if (!uuidFile.exists()) {
      throw new NotFoundException(String.format("No such uuid '%s' in Support Bundle.", uuid));
    }

    File statusFile = new File(uuidFile, "status.json");
    if (!statusFile.exists()) {
      throw new NotFoundException(
          String.format("The status for this bundle: %s is not found.", uuid));
    }
    SupportBundleStatus bundleStatus = getBundleStatus(uuidFile);
    Set<SupportBundleTaskStatus> supportBundleTaskStatusSet = bundleStatus.getTasks();
    return new SupportBundleOperationStatus(uuidFile.getName(), bundleStatus.getStatus(),
        supportBundleTaskStatusSet);
  }

  /**
   * Archive support bundle into zip file
   */
  public String createBundleZipByRequest(String uuid, Path tmpPath,
      SupportBundleRequestFileList bundleRequestFileList)
      throws IOException, NotFoundException, NoSuchAlgorithmException, BadRequestException {
    File uuidFile = getUUIDFile(uuid);
    if (!uuidFile.exists()) {
      throw new NotFoundException(String.format("This bundle id %s is not existed", uuid));
    }
    if (bundleRequestFileList == null || bundleRequestFileList.getFiles() == null) {
      throw new BadRequestException("The request body must contain a 'files' list.");
    }

    String bundleName = uuidFile.getName();
    Path bundleDir = uuidFile.getCanonicalFile().toPath();
    // Resolve every requested file before writing anything, so an invalid path fails fast.
    Map<String, File> requestedFiles = new LinkedHashMap<>();
    for (String filePath : bundleRequestFileList.getFiles()) {
      File requestFile = resolveBundleFile(bundleDir, filePath);
      if (requestFile.isFile()) {
        requestedFiles.putIfAbsent(getEntryName(bundleName, bundleDir, requestFile.toPath()),
            requestFile);
      }
    }

    MessageDigest digest = MessageDigest.getInstance("SHA-256");

    try (ZipOutputStream zipOut = new ZipOutputStream(
        new DigestOutputStream(Files.newOutputStream(tmpPath, StandardOpenOption.TRUNCATE_EXISTING),
            digest))) {
      if (bundleRequestFileList.getFiles().isEmpty()) {
        // An empty file list means the whole bundle directory is archived.
        addBundleToArchive(bundleName, bundleDir, zipOut);
      } else {
        for (Map.Entry<String, File> requestedFile : requestedFiles.entrySet()) {
          zipOut.putNextEntry(new ZipEntry(requestedFile.getKey()));
          Files.copy(requestedFile.getValue().toPath(), zipOut);
          zipOut.closeEntry();
        }
      }
    }
    return String.format("%s=%s", digest.getAlgorithm().toLowerCase(),
        Base64.getEncoder().encodeToString(digest.digest()));
  }

  /**
   * Get all bundles status
   */
  public List<SupportBundleOperationStatus> getAllBundleStatus() {
    File baseDirectory = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    if (!baseDirectory.exists()) {
      LOG.debug("No content in Support Bundle.");
      return new ArrayList<>();
    }
    List<SupportBundleOperationStatus> operationStatusList = new ArrayList<>();

    DirUtils.listFiles(baseDirectory)
        .stream()
        .filter(file -> !file.isHidden() && file.isDirectory())
        .forEach(uuidFile -> {
          SupportBundleOperationStatus operationStatus = new SupportBundleOperationStatus(
              uuidFile.getName(),
              CollectionState.NOT_FOUND,
              new HashSet<>());
          try {
            operationStatus = getBundle(uuidFile.getName());
          } catch (Exception e) {
            LOG.debug("Can not find status json: {}", uuidFile.getName());
          }
          operationStatusList.add(operationStatus);
        });
    return operationStatusList;
  }

  /**
   * Returns the bundle directory for the given uuid.
   *
   * @throws NotFoundException if the uuid is not a well-formed UUID, since no bundle can exist
   *     for it
   */
  private File getUUIDFile(String uuid) throws NotFoundException {
    if (!isValidUuid(uuid)) {
      throw new NotFoundException(String.format("No such uuid '%s' in Support Bundle.", uuid));
    }
    File baseDirectory = new File(cConf.get(Constants.SupportBundle.LOCAL_DATA_DIR));
    return new File(baseDirectory, uuid);
  }

  /**
   * Adds the bundle directory to the archive. Symbolic links are not followed and are skipped, so
   * only files that are inside the bundle directory are archived.
   */
  private static void addBundleToArchive(String bundleName, Path bundleDir,
      ZipOutputStream zipOut) throws IOException {
    Files.walkFileTree(bundleDir, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
          throws IOException {
        String entryName = getEntryName(bundleName, bundleDir, dir);
        zipOut.putNextEntry(new ZipEntry(entryName.endsWith("/") ? entryName : entryName + "/"));
        zipOut.closeEntry();
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (attrs.isRegularFile()) {
          zipOut.putNextEntry(new ZipEntry(getEntryName(bundleName, bundleDir, file)));
          Files.copy(file, zipOut);
          zipOut.closeEntry();
        }
        return FileVisitResult.CONTINUE;
      }
    });
  }

  /**
   * Returns the archive entry name of a path inside the bundle directory.
   */
  private static String getEntryName(String bundleName, Path bundleDir, Path path) {
    return bundleName + "/"
        + bundleDir.relativize(path).toString().replace(File.separatorChar, '/');
  }

  /**
   * Resolves a file path requested by the client against the canonical bundle directory.
   *
   * @throws BadRequestException if the path is malformed, or does not resolve to a path inside
   *     the bundle directory that is not a directory
   */
  private static File resolveBundleFile(Path bundleDir, @Nullable String filePath)
      throws IOException, BadRequestException {
    String invalidPathMessage = String.format("Invalid file path '%s' for support bundle '%s'.",
        filePath, bundleDir.getFileName());
    if (filePath == null || filePath.indexOf('\0') >= 0) {
      throw new BadRequestException(invalidPathMessage);
    }
    File requestFile = new File(bundleDir.toFile(), filePath).getCanonicalFile();
    if (!requestFile.toPath().startsWith(bundleDir) || requestFile.isDirectory()) {
      throw new BadRequestException(invalidPathMessage);
    }
    return requestFile;
  }

  /**
   * Validates the user supplied parameters in the bundle configuration.
   *
   * @throws BadRequestException if any of the parameters is invalid
   */
  public static void validateConfiguration(SupportBundleConfiguration config)
      throws BadRequestException {
    String namespace = config.getNamespace();
    if (namespace != null && !EntityId.isValidNamespace(namespace)) {
      throw new BadRequestException(String.format("Invalid namespace '%s'.", namespace));
    }
    String app = config.getApp();
    if (app != null && !EntityId.isValidId(app)) {
      throw new BadRequestException(String.format("Invalid application name '%s'.", app));
    }
    if (config.getProgramType() == null) {
      throw new BadRequestException("Program type must be provided.");
    }
    String programName = config.getProgramName();
    if (programName == null || !EntityId.isValidId(programName)) {
      throw new BadRequestException(String.format("Invalid program name '%s'.", programName));
    }
    if (config.getMaxRunsPerProgram() <= 0) {
      throw new BadRequestException(String.format(
          "Invalid maxRunsPerProgram '%d'. It must be a positive number.",
          config.getMaxRunsPerProgram()));
    }
    String run = config.getRun();
    if (run != null && !isValidUuid(run)) {
      throw new BadRequestException(String.format("Invalid run id '%s'.", run));
    }
  }

  /**
   * Returns whether the given string is a UUID in its canonical string form.
   */
  private static boolean isValidUuid(@Nullable String value) {
    if (value == null) {
      return false;
    }
    try {
      // UUID.fromString is lenient (e.g. it accepts "1-1-1-1-1"), so compare with the canonical
      // form to only accept the 8-4-4-4-12 hex representation.
      return UUID.fromString(value).toString().equalsIgnoreCase(value);
    } catch (IllegalArgumentException e) {
      return false;
    }
  }

  /**
   * Gets oldest folder from the root directory
   */
  private File getOldestFolder(File baseDirectory) {
    List<File> uuidFiles = DirUtils.listFiles(baseDirectory)
        .stream()
        .filter(file -> !file.isHidden() && file.isDirectory())
        .collect(Collectors.toList());
    return Collections.min(uuidFiles, Comparator.<File, Boolean>comparing(f1 -> {
      try {
        return getBundleStatus(f1).getStatus() != CollectionState.FAILED;
      } catch (Exception e) {
        throw new RuntimeException("Failed to get file status ", e);
      }
    }).thenComparing(File::lastModified));
  }

  /**
   * Gets latest folder from the root directory
   */
  private File getLatestFolder() {
    File baseDirectory = new File(localDir);
    if (DirUtils.listFiles(baseDirectory, f -> !f.isHidden() && f.isDirectory()).isEmpty()) {
      return null;
    }
    List<File> uuidFiles = DirUtils.listFiles(baseDirectory).stream()
        .filter(file -> !file.getName().startsWith(".") && !file.isHidden() && file.isDirectory())
        .collect(Collectors.toList());
    return Collections.max(uuidFiles, Comparator.comparing(File::lastModified));
  }

  /**
   * Update status file
   */
  private void addToStatus(SupportBundleStatus bundleStatus, String basePath) throws IOException {
    try (FileWriter statusFile = new FileWriter(
        new File(basePath, SupportBundleFileNames.STATUS_FILE_NAME))) {
      GSON.toJson(bundleStatus, statusFile);
    }
  }

  /**
   * read status file
   */
  private SupportBundleStatus readStatusJson(File statusFile) throws IOException {
    SupportBundleStatus bundleStatus;
    try (Reader reader = Files.newBufferedReader(statusFile.toPath(), StandardCharsets.UTF_8)) {
      bundleStatus = GSON.fromJson(reader, SupportBundleStatus.class);
    }
    return bundleStatus;
  }

  /**
   * valid if the namespace exists or not
   */
  private NamespaceId validNamespace(NamespaceId namespace) throws Exception {
    if (!namespaceQueryClient.exists(namespace)) {
      throw new NamespaceNotFoundException(namespace);
    }

    return namespace;
  }
}
