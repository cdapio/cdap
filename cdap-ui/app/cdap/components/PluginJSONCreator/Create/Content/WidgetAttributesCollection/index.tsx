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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';
import { WIDGET_TYPE_TO_ATTRIBUTES } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import AttributeKeyValueInput from 'components/PluginJSONCreator/Create/Content/WidgetAttributesCollection/AttributeKeyValueInput';
import AttributeMultipleValuesInput from 'components/PluginJSONCreator/Create/Content/WidgetAttributesCollection/AttributeMultipleValuesInput';
import WidgetInput from 'components/PluginJSONCreator/Create/Content/WidgetInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    attributeInput: {
      '& > *': {
        width: '100%',
        marginTop: '25px',
        marginBottom: '25px',
      },
    },
  };
};

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
    if (field == 'default' && codeEditors.includes(widgetType)) {
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
    const supportsMultipleTypes = supportedTypes ? supportedTypes.length > 1 : false;
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

const WidgetAttributesCollectionView: React.FC<WithStyles<typeof styles>> = ({
  classes,
  widgetObject,
  onNameChange,
  onLabelChange,
  onWidgetTypeChange,
  onWidgetCategoryChange,
  onAddWidget,
  onDeleteWidget,
  widgetID,
  onWidgetTypeChange,
  widgetToInfo,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  const widget = widgetToInfo[widgetID];
  const widgetType = widget ? widget.widgetType : null;
  const attributeFields =
    widgetToAttributes && widgetToAttributes[widgetID]
      ? Object.keys(widgetToAttributes[widgetID])
      : [];
  return (
    <div>
      <WidgetInput
        widgetObject={widgetToInfo[widgetID]}
        onNameChange={onNameChange}
        onLabelChange={onLabelChange}
        onWidgetTypeChange={onWidgetTypeChange}
        onWidgetCategoryChange={onWidgetCategoryChange}
        onAddWidget={onAddWidget}
        onDeleteWidget={onDeleteWidget}
      />
      {attributeFields.map((field) => {
        const fieldInfo = WIDGET_TYPE_TO_ATTRIBUTES[widgetType]
          ? WIDGET_TYPE_TO_ATTRIBUTES[widgetType][field]
          : {};
        return (
          <div className={classes.attributeInput}>
            <WidgetAttributeInput
              widgetType={widgetType}
              field={field}
              fieldInfo={fieldInfo}
              widgetToAttributes={widgetToAttributes}
              setWidgetToAttributes={setWidgetToAttributes}
              widgetID={widgetID}
            />
          </div>
        );
      })}
    </div>
  );
};

const WidgetAttributesCollection = withStyles(styles)(WidgetAttributesCollectionView);
export default WidgetAttributesCollection;
