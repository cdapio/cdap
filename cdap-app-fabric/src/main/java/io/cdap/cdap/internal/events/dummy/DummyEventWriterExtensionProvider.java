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

package io.cdap.cdap.internal.events.dummy;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;
import io.cdap.cdap.internal.events.EventWriterProvider;
import io.cdap.cdap.spi.events.EventWriter;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DummyEventWriterExtensionProvider implements EventWriterProvider {

    private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();

    private final DummyEventWriter eventWriter;

    private static Set<String> createAllowedResources() {
        try {
            return ClassPathResources.getResourcesWithDependencies(EventWriter.class.getClassLoader(),
                    EventWriter.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to trace dependencies for provisioner extension. " +
                    "Usage of metrics writer might fail.", e);
        }
    }

    @Inject
    public DummyEventWriterExtensionProvider(DummyEventWriter eventWriter) {
        System.out.println("Injecting extension provider");
        this.eventWriter = eventWriter;
    }

    @Override
    public Map<String, EventWriter> loadEventWriters() {
        Map<String, EventWriter> map = new HashMap<>();
        map.put(this.eventWriter.getID(), this.eventWriter);
        System.out.println("Loading event writers: " + this.eventWriter.getID());
        return Collections.unmodifiableMap(map);
    }
}
