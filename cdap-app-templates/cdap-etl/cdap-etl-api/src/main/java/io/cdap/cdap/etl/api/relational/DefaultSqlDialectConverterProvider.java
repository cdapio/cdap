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
import com.google.inject.Provider;
import com.google.inject.name.Named;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;

import javax.annotation.Nullable;

public class DefaultSqlDialectConverterProvider implements Provider<SqlDialectConverter> {

    public static final String CONVERTER_NAME_KEY = "ConverterNameKey";

    @Nullable
    private final String defaultConverterName;
    private final SqlDialectConverterExtensionLoader extensionLoader;

    @Inject
    DefaultSqlDialectConverterProvider(CConfiguration cconf,
                                       @Named(CONVERTER_NAME_KEY) String sqlDialectConverterNameKey,
                                       SqlDialectConverterExtensionLoader extensionLoader) {
        String converterName = cconf.get(sqlDialectConverterNameKey);
        defaultConverterName = converterName == null ?
                cconf.get(Constants.SqlDialectConversion.DEFAULT_IMPL_NAME) :
                converterName;
        this.extensionLoader = extensionLoader;
    }

    /**
     * Retrieves the current {@link SqlDialectConverter} from the extension loader using the converter name or
     * {@code null} if there is no current converter available.
     */
    @Override
    public SqlDialectConverter get() {
        if (defaultConverterName != null) {
            SqlDialectConverter sqlDialectConverter = extensionLoader.get(defaultConverterName);
            return sqlDialectConverter;
        }
        return null;
    }
}
