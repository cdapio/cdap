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

package io.cdap.cdap.etl.api.relational;

import com.google.inject.Inject;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.common.lang.ClassPathResources;
import io.cdap.cdap.common.lang.FilterClassLoader;
import io.cdap.cdap.extension.AbstractExtensionLoader;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

public class SqlDialectConverterExtensionLoader extends AbstractExtensionLoader<String, SqlDialectConverter> {

    private static final Set<String> ALLOWED_RESOURCES = createAllowedResources();
    private static final Set<String> ALLOWED_PACKAGES = createPackageSets(ALLOWED_RESOURCES);

    private static Set<String> createAllowedResources() {
        // Only allow SQL dialect converter classes.
        try {
            return ClassPathResources.getResourcesWithDependencies(
                    SqlDialectConverterExtensionLoader.class.getClassLoader(),
                    SqlDialectConverter.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to trace dependencies for SQL dialect converter extension. " +
                    "Usage of SQL dialect converter might fail.", e);
        }
    }

    @Inject
    public SqlDialectConverterExtensionLoader(CConfiguration cConf) {
        super(cConf.get(Constants.SqlDialectConversion.EXTENSION_DIR));
    }

    @Override
    protected Set<String> getSupportedTypesForProvider(SqlDialectConverter sqlDialectConverter) {
        return Collections.singleton(sqlDialectConverter.getName());
    }

    @Override
    protected FilterClassLoader.Filter getExtensionParentClassLoaderFilter() {
        return new FilterClassLoader.Filter() {
            @Override
            public boolean acceptResource(String resource) {
                return ALLOWED_RESOURCES.contains(resource);
            }

            @Override
            public boolean acceptPackage(String packageName) {
                return ALLOWED_PACKAGES.contains(packageName);
            }
        };
    }
}
