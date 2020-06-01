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

<<<<<<< HEAD
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { CODE_EDITORS } from 'components/PluginJSONCreator/constants';
=======
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import AttributeKeyValueInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/AttributeKeyValueInput';
import AttributeMultipleValuesInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/AttributeMultipleValuesInput';
import * as React from 'react';

<<<<<<< HEAD
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
=======
const WidgetAttributeInput = ({
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
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

<<<<<<< HEAD
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

=======
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
  const renderAttributeInput = () => {
    if (!fieldInfo) {
      return;
    }
<<<<<<< HEAD

    if (field === 'default' && CODE_EDITORS.includes(widgetType)) {
=======
    const codeEditors = [
      'javascript-editor',
      'json-editor',
      'python-editor',
      'scala-editor',
      'sql-editor',
    ];
    if (field === 'default' && codeEditors.includes(widgetType)) {
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
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
<<<<<<< HEAD
    if (isMultipleInput) {
=======
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
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
      return (
        <AttributeMultipleValuesInput
          supportedTypes={supportedTypes}
          widgetToAttributes={widgetToAttributes}
          setWidgetToAttributes={setWidgetToAttributes}
          widgetID={widgetID}
          field={field}
        />
      );
<<<<<<< HEAD
    } else {
      return generateAttributeInput(fieldInfo.type);
    }
  };

  return <div className={classes.widgetAttributeINput}>{renderAttributeInput()}</div>;
};

const WidgetAttributeInput = withStyles(styles)(WidgetAttributeInputView);
=======
    }
  };

  return <div>{renderAttributeInput()}</div>;
};

>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
export default WidgetAttributeInput;
