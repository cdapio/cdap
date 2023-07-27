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

package io.cdap.cdap.master.spi.twill;

import io.cdap.cdap.master.spi.autoscaler.MetricsEmitter;
import org.apache.twill.api.TwillContext;
import org.apache.twill.api.TwillRunnable;

/**
 * Extension interface for {@link TwillRunnable} to add extra functionalities for CDAP.
 */

public interface AutoscalableTwillRunnable extends TwillRunnable {

    /**
     * Works for sending the metrics to KubeTwillLauncher through StackdriverMetricsEmitter interface.
     * @param metricsEmitter StackdriverMetricsEmitter instance of the autoscaler
     *                    metrics which is to be emitted through Cloud Monitoring Engine.
     * @throws Exception throws IOException.
     */

    void initialize(TwillContext context, MetricsEmitter metricsEmitter);
}
