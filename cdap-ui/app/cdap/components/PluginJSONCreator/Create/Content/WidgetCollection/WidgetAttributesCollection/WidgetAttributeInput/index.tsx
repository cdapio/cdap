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

import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import AttributeKeyValueInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/AttributeKeyValueInput';
import AttributeMultipleValuesInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/AttributeMultipleValuesInput';
import * as React from 'react';

const WidgetAttributeInput = ({
  field,
  fieldInfo,
  widgetToAttributes,
  setWidgetToAttributes,
  widgetID,
  widgetType,
}) => {
  const onAttributeChange = (newVal) => {
    setWidgetToAttributes((prevObjs) => ({
      ...prevObjs,
      [widgetID]: { ...prevObjs[widgetID], [field]: newVal },
    }));
  };

  const renderAttributeInput = () => {
    if (!fieldInfo) {
      return;
    }
    const codeEditors = [
      'javascript-editor',
      'json-editor',
      'python-editor',
      'scala-editor',
      'sql-editor',
    ];
    if (codeEditors.includes(widgetType)) {
      return (
        <WidgetWrapper
          widgetProperty={{
            field,
            name: field,
            'widget-type': widgetType,
            'widget-attributes': {},
          }}
          pluginProperty={{
            required: fieldInfo.required,
            name: 'code-editor',
          }}
          value={widgetToAttributes[widgetID][field]}
          onChange={onAttributeChange}
        />
      );
    }

    const isMultipleInput = fieldInfo.type.includes('[]');
    const supportedTypes = fieldInfo.type.split('|');
    if (!isMultipleInput) {
      switch (fieldInfo.type) {
        case 'string':
          return (
            <PluginInput
              widgetType={'textbox'}
              value={widgetToAttributes[widgetID][field]}
              setValue={onAttributeChange}
              label={field}
              required={fieldInfo.required}
            />
          );
        case 'number':
          return (
            <PluginInput
              widgetType={'number'}
              value={widgetToAttributes[widgetID][field]}
              setValue={onAttributeChange}
              label={field}
              required={fieldInfo.required}
            />
          );
        case 'boolean':
          return (
            <PluginInput
              widgetType={'select'}
              value={widgetToAttributes[widgetID][field]}
              setValue={onAttributeChange}
              label={field}
              options={['true', 'false']}
              required={fieldInfo.required}
            />
          );
        case 'IToggle':
          return (
            <AttributeKeyValueInput
              keyField={'label'}
              valueField={'value'}
              keyRequired={true}
              valueRequired={true}
              widgetToAttributes={widgetToAttributes}
              setWidgetToAttributes={setWidgetToAttributes}
              widgetID={widgetID}
              field={field}
            />
          );
        case 'IOption':
          return (
            <AttributeKeyValueInput
              keyField={'id'}
              valueField={'label'}
              keyRequired={true}
              valueRequired={true}
              widgetToAttributes={widgetToAttributes}
              setWidgetToAttributes={setWidgetToAttributes}
              widgetID={widgetID}
              field={field}
            />
          );
        default:
          return (
            <PluginInput
              widgetType={'textbox'}
              value={widgetToAttributes[widgetID][field]}
              setValue={onAttributeChange}
              label={field}
              required={fieldInfo.required}
            />
          );
      }
    } else {
      return (
        <AttributeMultipleValuesInput
          supportedTypes={supportedTypes}
          widgetToAttributes={widgetToAttributes}
          setWidgetToAttributes={setWidgetToAttributes}
          widgetID={widgetID}
          field={field}
        />
      );
    }
  };

  return <div>{renderAttributeInput()}</div>;
};

export default WidgetAttributeInput;
