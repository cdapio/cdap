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

package io.cdap.cdap.common.internal.relational;

import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import io.cdap.cdap.etl.spi.relational.SqlDialectConverter;

/**
 * Provides Guice bindings for {@link SqlDialectConverter}.
 */
public final class SqlDialectConverterModules {

    private SqlDialectConverterModules() {}

    /**
     * Returns the default bindings for the {@link SqlDialectConverter}.
     * @return A module with {@link SqlDialectConverter} bindings.
     */
    public static Module getDefaultModule() {
        return getDefaultModule();
    }

    public static Module getDefaultModule(String sqlDialectConverterNameKey) {
        return new PrivateModule() {
            @Override
            protected void configure() {
                bind(String.class)
                        .annotatedWith(Names.named(DefaultSqlDialectConverterProvider.CONVERTER_NAME_KEY))
                        .toInstance(sqlDialectConverterNameKey);
                bind(SqlDialectConverterExtensionLoader.class).in(Scopes.SINGLETON);
                bind(SqlDialectConverter.class).toProvider(DefaultSqlDialectConverterProvider.class);
                expose(SqlDialectConverter.class);
            }
        };
    }

}
