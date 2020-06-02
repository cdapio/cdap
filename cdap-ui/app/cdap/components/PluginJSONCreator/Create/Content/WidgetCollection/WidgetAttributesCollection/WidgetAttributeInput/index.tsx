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

import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { CODE_EDITORS } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import AttributeKeyValueInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/AttributeKeyValueInput';
import AttributeMultipleValuesInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/AttributeMultipleValuesInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetAttributeInput: {
      width: '100%',
      marginTop: '10px',
      marginBottom: '10px',
    },
  };
};

const WidgetAttributeInputView = ({
  classes,
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

  const generateAttributeInput = (fieldType) => {
    if (fieldType === 'IToggle' || fieldType === 'IOption') {
      const props = {
        keyRequired: true,
        valueRequired: true,
        widgetToAttributes,
        setWidgetToAttributes,
        widgetID,
        field,
      };

      const finalProps = {
        ...props,
        ...(fieldType === 'IToggle' && { keyField: 'label', valueField: 'value' }),
        ...(fieldType === 'IOption' && { keyField: 'id', valueField: 'label' }),
      };

      return <AttributeKeyValueInput {...finalProps} />;
    } else {
      const props = {
        label: field,
        value: widgetToAttributes[widgetID][field],
        onChange: onAttributeChange,
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

  const renderAttributeInput = () => {
    if (!fieldInfo) {
      return;
    }

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
          value={widgetToAttributes[widgetID][field]}
          onChange={onAttributeChange}
        />
      );
    }

    const isMultipleInput = fieldInfo.type.includes('[]');
    const supportedTypes = fieldInfo.type.split('|');
    if (isMultipleInput) {
      return (
        <AttributeMultipleValuesInput
          supportedTypes={supportedTypes}
          widgetToAttributes={widgetToAttributes}
          setWidgetToAttributes={setWidgetToAttributes}
          widgetID={widgetID}
          field={field}
        />
      );
    } else {
      return generateAttributeInput(fieldInfo.type);
    }
  };

  return <div className={classes.widgetAttributeINput}>{renderAttributeInput()}</div>;
};

const WidgetAttributeInput = withStyles(styles)(WidgetAttributeInputView);
export default WidgetAttributeInput;
