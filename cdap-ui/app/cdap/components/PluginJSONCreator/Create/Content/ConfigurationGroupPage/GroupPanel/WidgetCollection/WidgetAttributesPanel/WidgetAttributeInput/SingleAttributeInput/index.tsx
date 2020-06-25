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
import { CODE_EDITORS } from 'components/PluginJSONCreator/constants';
import AttributeKeyValueInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/SingleAttributeInput/AttributeKeyValueInput';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import * as React from 'react';

export const SingleAttributeInput = ({
  widgetID,
  widgetType,
  fieldInfo,
  field,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  const fieldType = fieldInfo.type;

  const currentAttributeValue = localWidgetToAttributes.get(widgetID)
    ? localWidgetToAttributes.get(widgetID).get(field)
    : '';

  const onAttributeValueChange = (newVal) => {
    setLocalWidgetToAttributes(localWidgetToAttributes.setIn([widgetID, field], newVal));
  };

  const generateSingleAttributeInput = () => {
    if (field === 'default' && CODE_EDITORS.includes(widgetType)) {
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
          value={currentAttributeValue}
          onChange={onAttributeValueChange}
        />
      );
    }

    if (fieldType === 'IToggle' || fieldType === 'IOption') {
      const props = {
        keyRequired: true,
        valueRequired: true,
        widgetID,
        field,
        localWidgetToAttributes,
        setLocalWidgetToAttributes,
      };

      const finalProps = {
        ...props,
        ...(fieldType === 'IToggle' && { keyField: 'label', valueField: 'value' }),
        ...(fieldType === 'IOption' && { keyField: 'id', valueField: 'label' }),
      };

      return (
        <div>
          <AttributeKeyValueInput {...finalProps} />
        </div>
      );
    } else {
      const props = {
        label: field,
        value: currentAttributeValue,
        onChange: onAttributeValueChange,
        required: fieldInfo.required,
        // Set default widgetType in case fieldType is invalid
        widgetType: 'textbox',
      };

      const finalProps = {
        ...props,
        ...(fieldType === 'string' && { widgetType: 'textbox' }),
        ...(fieldType === 'number' && { widgetType: 'number' }),
        ...(fieldType === 'boolean' && { widgetType: 'select', options: ['true', 'false'] }),
      };

      return <PluginInput {...finalProps} />;
    }
  };

  return generateSingleAttributeInput();
};

export default SingleAttributeInput;
