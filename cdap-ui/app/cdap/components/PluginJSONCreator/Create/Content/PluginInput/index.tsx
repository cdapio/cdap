/*
 * Copyright © 2020 Cask Data, Inc.
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

import * as React from 'react';

import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';

const PluginInput = ({
  widgetType,
  onChange,
  value,
  label,
  placeholder = '',
  delimeter = null,
  options = null,
  required = false,
  keyPlaceholder = null,
  valuePlaceholder = null,
  kvDelimiter = null,
  layout = null,
  delimiter = null,
  focus = false,
}) => {
  let widgetAttributes;
  if (widgetType !== 'toggle') {
    widgetAttributes = {
      ...(layout && { layout }),
      ...(delimeter && { delimeter }),
      ...(options && { options }),
      ...(placeholder && { placeholder }),
      ...(value && { default: value }),
      ...(keyPlaceholder && { 'key-placeholder': keyPlaceholder }),
      ...(valuePlaceholder && { 'value-placeholder': valuePlaceholder }),
      ...(kvDelimiter && { 'kv-delimiter': kvDelimiter }),
      ...(delimiter && { delimiter }),
    };
  } else {
    widgetAttributes = {
      on: {
        value: 'true',
        label: 'true',
      },
      off: {
        value: 'false',
        label: 'false',
      },
      default: value ? 'true' : 'false',
    };
  }

  const widget = {
    label,
    name: label,
    'widget-type': widgetType,
    'widget-attributes': widgetAttributes,
  };

  const property = {
    required,
    name: label.toLowerCase().replace(/\s/g, '-'),
  };

  return (
    <WidgetWrapper
      widgetProperty={widget}
      pluginProperty={property}
      value={value}
      onChange={onChange}
      focus={focus}
    />
  );
};

export default PluginInput;
