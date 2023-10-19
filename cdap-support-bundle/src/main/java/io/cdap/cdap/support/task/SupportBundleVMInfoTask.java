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

package io.cdap.cdap.support.task;

import com.google.gson.Gson;
import io.cdap.cdap.common.NotFoundException;
import io.cdap.cdap.api.service.ServiceUnavailableException;
import io.cdap.cdap.common.healthcheck.VMInformation;
import io.cdap.cdap.common.healthcheck.VMInformationFetcher;
import io.cdap.cdap.common.utils.DirUtils;
import io.cdap.cdap.proto.SystemServiceMeta;
import io.cdap.cdap.support.metadata.RemoteMonitorServicesFetcher;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.management.MemoryUsage;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link SupportBundleTask} that collects VM information across different services.
 */
public class SupportBundleVMInfoTask implements SupportBundleTask {

  private static final Logger LOG = LoggerFactory.getLogger(SupportBundleVMInfoTask.class);
  private static final Gson GSON = new Gson();

  private final RemoteMonitorServicesFetcher servicesFetcher;
  private final VMInformationFetcher vmInfoFetcher;
  private final File baseDir;

  public SupportBundleVMInfoTask(RemoteMonitorServicesFetcher servicesFetcher,
      VMInformationFetcher vmInfoFetcher,
      File baseDir) {
    this.servicesFetcher = servicesFetcher;
    this.vmInfoFetcher = vmInfoFetcher;
    this.baseDir = baseDir;
  }

  @Override
  public void collect() throws IOException, NotFoundException {
    for (SystemServiceMeta serviceMeta : servicesFetcher.listSystemServices()) {
      String service = serviceMeta.getName();
      File vmInfoDir = new File(new File(baseDir, "vminfo"), service);
      DirUtils.mkdirs(vmInfoDir);

      try {
        VMInformation vmInfo = vmInfoFetcher.getVMInformation(service);

        File memoryUsageFile = new File(vmInfoDir, "memory.txt");
        try (Writer writer = Files.newBufferedWriter(memoryUsageFile.toPath(),
            StandardCharsets.UTF_8)) {
          Map<String, MemoryUsage> memoryUsages = new HashMap<>();
          memoryUsages.put("heap", vmInfo.getHeapMemoryUsage());
          memoryUsages.put("nonheap", vmInfo.getNonHeapMemoryUsage());
          GSON.toJson(memoryUsages, writer);
        }

        File threadDumpFile = new File(vmInfoDir, "threadDump.txt");
        try (Writer writer = Files.newBufferedWriter(threadDumpFile.toPath(),
            StandardCharsets.UTF_8)) {
          writer.write(vmInfo.getThreads());
        }
      } catch (ServiceUnavailableException e) {
        LOG.warn("Service {} is not available. Skipping VM information.", service);
        LOG.debug("Service {} is not available", service, e);
      }
    }
  }
}
