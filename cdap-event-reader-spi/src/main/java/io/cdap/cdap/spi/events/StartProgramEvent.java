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

package io.cdap.cdap.spi.events;

import javax.annotation.Nullable;

public class StartProgramEvent implements Event<StartProgramEventDetails> {

    private final long publishTime;
    private final String version;
    private final String projectName;
    private final StartProgramEventDetails startProgramEventDetails;

    public StartProgramEvent(long publishTime, String version,
                             @Nullable String projectName,
                             StartProgramEventDetails startProgramEventDetails) {
        this.publishTime = publishTime;
        this.version = version;
        this.projectName = projectName;
        this.startProgramEventDetails = startProgramEventDetails;
    }

    @Override
    public EventType getType() {
        return EventType.PROGRAM_START;
    }

    @Override
    public long getPublishTime() {
        return publishTime;
    }

    @Override
    public String getVersion() {
        return version;
    }

    @Nullable
    @Override
    public String getInstanceName() {
        return null;
    }

    @Nullable
    @Override
    public String getProjectName() {
        return projectName;
    }

    @Override
    public StartProgramEventDetails getEventDetails() {
        return startProgramEventDetails;
    }
}
