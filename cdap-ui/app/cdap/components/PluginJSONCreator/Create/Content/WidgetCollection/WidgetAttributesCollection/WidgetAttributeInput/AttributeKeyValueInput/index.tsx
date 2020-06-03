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

const AttributeKeyValueInput = ({
  keyField,
  valueField,
  keyRequired,
  valueRequired,
  widgetToAttributes,
  setWidgetToAttributes,
  widgetID,
  field,
}) => {
  const onKeyChange = (newVal) => {
    setWidgetToAttributes((prevObjs) => ({
      ...prevObjs,
      [widgetID]: {
        ...prevObjs[widgetID],
        [field]: {
          ...prevObjs[widgetID][field],
          [keyField]: newVal,
        },
      },
    }));
  };

  const onValueChange = (newVal) => {
    setWidgetToAttributes((prevObjs) => ({
      ...prevObjs,
      [widgetID]: {
        ...prevObjs[widgetID],
        [field]: {
          ...prevObjs[widgetID][field],
          [valueField]: newVal,
        },
      },
    }));
  };

  const keyWidget = {
    label: field + ' ' + keyField,
    name: keyField,
    'widget-type': 'textbox',
    'widget-attributes': {},
  };

  const valueWidget = {
    label: field + ' ' + valueField,
    name: keyField,
    'widget-type': 'textbox',
    'widget-attributes': {},
  };

  const keyProperty = {
    required: keyRequired,
    name: keyField,
  };

  const valueProperty = {
    required: valueRequired,
    name: valueField,
  };

  const currentKey = widgetToAttributes[widgetID][field][keyField]
    ? widgetToAttributes[widgetID][field][keyField]
    : '';
  const currentValue = widgetToAttributes[widgetID][field][valueField]
    ? widgetToAttributes[widgetID][field][valueField]
    : '';

  return (
    <div>
      <WidgetWrapper
        widgetProperty={keyWidget}
        pluginProperty={keyProperty}
        value={currentKey}
        onChange={onKeyChange}
      />

      <WidgetWrapper
        widgetProperty={valueWidget}
        pluginProperty={valueProperty}
        value={currentValue}
        onChange={onValueChange}
      />
    </div>
  );
};

export default AttributeKeyValueInput;
