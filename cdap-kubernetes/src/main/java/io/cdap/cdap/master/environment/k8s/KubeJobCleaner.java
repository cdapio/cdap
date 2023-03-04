/*
 * Copyright © 2021 Cask Data, Inc.
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

package io.cdap.cdap.master.environment.k8s;

import io.cdap.cdap.master.spi.environment.MasterEnvironmentContext;
import io.cdap.cdap.master.spi.environment.MasterEnvironmentTask;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.BatchV1Api;
import io.kubernetes.client.openapi.models.V1DeleteOptions;
import io.kubernetes.client.openapi.models.V1Job;
import io.kubernetes.client.openapi.models.V1JobList;
import io.kubernetes.client.openapi.models.V1JobStatus;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kubernetes Job cleaner that scans and deletes completed jobs across all namespaces.
 */
class KubeJobCleaner implements MasterEnvironmentTask {

  private static final Logger LOG = LoggerFactory.getLogger(KubeJobCleaner.class);
  // The BatchV1Api client for interacting with the Kube API server. This needs to be a volatile to safeguard against
  // multiple concurrent client instance creation.
  private volatile BatchV1Api batchV1Api;
  private final String selector;
  private final int batchSize;
  private final long delayMillis;
  private final ApiClientFactory apiClientFactory;

  KubeJobCleaner(String selector, int batchSize, long delayMin, ApiClientFactory apiClientFactory) {
    this.selector = selector;
    this.batchSize = batchSize;
    this.delayMillis = TimeUnit.MINUTES.toMillis(delayMin);
    this.apiClientFactory = apiClientFactory;
  }

  @Override
  public long run(MasterEnvironmentContext context) {
    String continuationToken = null;
    int retryCount = 10;
    int jobDeletionCount = 0;
    do {
      try {
        batchV1Api = getBatchV1Api();
        // Attempt to delete completed jobs. K8s current implementation only supports status.successful field selector
        // for jobs. https://github.com/kubernetes/kubernetes/blob/master/pkg/apis/batch/v1/conversion.go
        // so instead, list all the jobs in all k8s namespace and delete completed (successful + failed) jobs one
        // by one.
        V1JobList jobs = batchV1Api.listJobForAllNamespaces(null, continuationToken, null, selector,
            batchSize, null,
            null, null, (int) TimeUnit.MINUTES.toSeconds(10), null);
        for (V1Job job : jobs.getItems()) {
          V1JobStatus jobStatus = job.getStatus();
          // Only attempt to delete completed jobs.
          if (jobStatus != null && (jobStatus.getSucceeded() != null
              || jobStatus.getFailed() != null)) {
            String jobName = job.getMetadata().getName();
            String kubeNamespace = job.getMetadata().getNamespace();
            V1DeleteOptions v1DeleteOptions = new V1DeleteOptions();
            v1DeleteOptions.setPropagationPolicy("Background");
            try {
              // Rely on k8s garbage collector to delete dependent pods in background while job resource is deleted
              // immediately - https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection
              LOG.debug("Cleaning up job {} in kubernetes environment", jobName);
              batchV1Api.deleteNamespacedJob(jobName, kubeNamespace, null, null, null, null, null,
                  v1DeleteOptions);
              jobDeletionCount++;
            } catch (ApiException e) {
              if (e.getCode() == 404) {
                // Ignore if status code is 404, this could happen in case there is some race condition while issuing
                // delete for the same job.
                LOG.trace("Ignoring job deletion for job {} because job was not found.", jobName,
                    e);
              } else {
                // catch the exception so that we can proceed with other job deletions.
                LOG.warn(
                    "Failed to cleanup job resources for job {}. This attempt will be retried later.",
                    jobName, e);
              }
            }
          }
        }
        continuationToken = jobs.getMetadata().getContinue();
      } catch (IOException | ApiException e) {
        // This could happen if there was error while listing the jobs or creating batch api client.
        retryCount--;
        try {
          Thread.sleep(200);
        } catch (InterruptedException ex) {
          // If interrupted during sleep, just break the loop
          break;
        }
        LOG.warn("Error while listing jobs or creating batch api client for cleanup, "
            + "this attempt will be retried.", e);
      }
    } while (retryCount != 0 && continuationToken != null);

    LOG.trace("Completed an iteration of job clean by removing {} number of jobs.",
        jobDeletionCount);
    return delayMillis;
  }

  private BatchV1Api getBatchV1Api() throws IOException {
    BatchV1Api api = batchV1Api;
    if (api != null) {
      return api;
    }

    synchronized (this) {
      api = batchV1Api;
      if (api != null) {
        return api;
      }
      batchV1Api = api = new BatchV1Api(apiClientFactory.create());
      return api;
    }
  }
}
