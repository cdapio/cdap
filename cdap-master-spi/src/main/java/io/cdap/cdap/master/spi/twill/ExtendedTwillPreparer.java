/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.master.spi.twill;


import java.util.Map;
import org.apache.twill.api.TwillPreparer;

/**
 * An extension of TwillPreparer used to add extra functionalities for CDAP.
 */
public interface ExtendedTwillPreparer extends TwillPreparer {

  /**
   * Set size limit for workdir volume in kube twill application which is an
   * emptydir.
   *
   * @param sizeLimitInMiB volume size limit in Mega Bytes
   */
  ExtendedTwillPreparer setWorkdirSizeLimit(int sizeLimitInMiB);

  /**
   * Sets whether config files such as cdap-site.xml should be localized using
   * kubernetes configmaps instead of having the init container fetch them. This
   * ensures that the init container runs with the same configuration as the
   * twill runables. Configmaps have a size limit of 1MiB, so they can't be used
   * to localize larger files.
   *
   * @param shouldLocalizeConfigurationAsConfigmap whether to localize config
   *                                               files using configmaps
   */
  ExtendedTwillPreparer setShouldLocalizeConfigurationAsConfigmap(
      boolean shouldLocalizeConfigurationAsConfigmap);

  /**
   * Creates the probes based on the Probe Config passed and assigns to the given runnable.
   * Example : It can be used for a Liveness Probe in k8s
   */
  ExtendedTwillPreparer addProbes(String runnableName, Map<String, String> probeConf);

  /**
   * Requests a deployment strategy that never runs two generations of the runnable's pod at the
   * same time.
   *
   * <p>The Kubernetes default RollingUpdate strategy derives {@code maxSurge} from 25% of the
   * replica count rounded <em>up</em>, and {@code maxUnavailable} from 25% rounded <em>down</em>.
   * At a single replica that resolves to {@code maxSurge=1, maxUnavailable=0}, so an update
   * creates the replacement pod and waits for it to become ready <em>before</em> deleting the
   * original. For most services that overlap is desirable. For a runnable that owns unshared
   * in-memory state, it means two instances briefly disagree about that state.
   *
   * <p>Callers that need at-most-one semantics must use this rather than relying on a replica
   * count of one, which constrains the steady state but not the transition.
   */
  ExtendedTwillPreparer withRecreateStrategy();
}

