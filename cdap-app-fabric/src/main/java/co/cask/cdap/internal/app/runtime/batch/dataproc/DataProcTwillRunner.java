/*
 * Copyright © 2018 Cask Data, Inc.
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

package co.cask.cdap.internal.app.runtime.batch.dataproc;

import org.apache.twill.api.ResourceSpecification;
import org.apache.twill.api.RunId;
import org.apache.twill.api.SecureStoreUpdater;
import org.apache.twill.api.TwillApplication;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillPreparer;
import org.apache.twill.api.TwillRunnable;
import org.apache.twill.api.TwillRunner;
import org.apache.twill.api.security.SecureStoreRenewer;
import org.apache.twill.common.Cancellable;

import java.util.concurrent.TimeUnit;

/**
 *
 */
public class DataProcTwillRunner implements TwillRunner {

    @Override
    public TwillPreparer prepare(TwillRunnable twillRunnable) {
        return null;
    }

    @Override
    public TwillPreparer prepare(TwillRunnable twillRunnable, ResourceSpecification resourceSpecification) {
        return null;
    }

    @Override
    public TwillPreparer prepare(TwillApplication twillApplication) {
        return null;
    }

    @Override
    public TwillController lookup(String s, RunId runId) {
        return null;
    }

    @Override
    public Iterable<TwillController> lookup(String s) {
        return null;
    }

    @Override
    public Iterable<LiveInfo> lookupLive() {
        return null;
    }

    @Override
    public Cancellable scheduleSecureStoreUpdate(SecureStoreUpdater secureStoreUpdater,
            long l, long l1, TimeUnit timeUnit) {
        return null;
    }

    @Override
    public Cancellable setSecureStoreRenewer(SecureStoreRenewer secureStoreRenewer,
            long l, long l1, long l2, TimeUnit timeUnit) {
        return null;
    }
}
