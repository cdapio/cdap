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

import { withStyles } from '@material-ui/core';
import { StyleRules } from '@material-ui/core/styles';
import If from 'components/If';
import KeyValuePairs from 'components/KeyValuePairs';
import { COMMON_DELIMITER } from 'components/PluginJSONCreator/constants';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import * as React from 'react';

export enum SupportedType {
  String = 'string',
  Number = 'number',
  ValueLabelPair = 'Value-Label pair',
  IDLabelPair = 'ID-Label pair',
}

const styles = (): StyleRules => {
  return {
    typeSelectInput: {
      width: '25%',
      marginTop: '20px',
      marginBottom: '20px',
    },
  };
};

export const processSupportedTypes = (types: string[]) => {
  const allTypes = new Set();
  types.forEach((type) => {
    switch (type) {
      case 'string[]':
        allTypes.add(SupportedType.String);
        break;
      case 'number[]':
        allTypes.add(SupportedType.Number);
        break;
      case 'IOption[]':
        allTypes.add(SupportedType.IDLabelPair);
        break;
      case 'IDropdownOption[]':
        allTypes.add(SupportedType.String);
        allTypes.add(SupportedType.Number);
        allTypes.add(SupportedType.ValueLabelPair);
        break;
      case 'ISelectOptions[]':
        allTypes.add(SupportedType.ValueLabelPair);
        break;
      case 'FilterOption[]':
        allTypes.add(SupportedType.String);
        allTypes.add(SupportedType.IDLabelPair);
        break;
      default:
        break;
    }
  });
  return allTypes;
};

const AttributeMultipleValuesInput = ({
  classes,
  supportedTypes,
  widgetToAttributes,
  setWidgetToAttributes,
  widgetID,
  field,
}) => {
  const cleanSupportedTypes = Array.from(processSupportedTypes(supportedTypes).values());
  const [selectedType, setSelectedType] = React.useState(cleanSupportedTypes[0]);

  function getLocalState(newType) {
    const widgetAttributes = widgetToAttributes[widgetID][field];
    if (!newType) {
      return '';
    } else if (newType === SupportedType.Number || newType === SupportedType.String) {
      return widgetAttributes ? widgetAttributes.join(COMMON_DELIMITER) : '';
    } else {
      if (widgetAttributes && widgetAttributes.length > 0) {
        return {
          pairs: widgetAttributes.map((keyvalue) => {
            if (keyvalue.id) {
              return {
                key: keyvalue.id,
                value: keyvalue.label,
              };
            } else {
              return {
                key: keyvalue.value,
                value: keyvalue.label,
              };
            }
          }),
        };
      } else {
        return { pairs: [{ key: '', value: '' }] };
      }
    }
  }

  const [localValues, setLocalValues] = React.useState(getLocalState(selectedType));

  const switchInputType = (newType) => {
    setSelectedType(newType);
    setLocalValues(getLocalState(newType));
    setWidgetToAttributes((prevObjs) => ({
      ...prevObjs,
      [widgetID]: { ...prevObjs[widgetID], [field]: [] },
    }));
  };

  const onAttributeChange = (newVal) => {
    setLocalValues(newVal);
    setWidgetToAttributes((prevObjs) => ({
      ...prevObjs,
      [widgetID]: { ...prevObjs[widgetID], [field]: newVal.split(COMMON_DELIMITER) },
    }));
  };

  const onKeyValueAttributeChange = (keyvalue, selectedType) => {
    if (selectedType === SupportedType.ValueLabelPair) {
      const keyvaluePairs = keyvalue.pairs.map((pair) => {
        return { value: pair.key, label: pair.value };
      });
      setWidgetToAttributes((prevObjs) => ({
        ...prevObjs,
        [widgetID]: { ...prevObjs[widgetID], [field]: keyvaluePairs },
      }));
    } else {
      const keyvaluePairs = keyvalue.pairs.map((pair) => {
        return { id: pair.key, label: pair.value };
      });
      setWidgetToAttributes((prevObjs) => ({
        ...prevObjs,
        [widgetID]: { ...prevObjs[widgetID], [field]: keyvaluePairs },
      }));
    }
  };

  const renderAttributeMultipleValuesInput = () => {
    const widgetAttributes = widgetToAttributes[widgetID][field];
    if (selectedType === SupportedType.ValueLabelPair) {
      return (
        <KeyValuePairs
          keyValues={localValues}
          onKeyValueChange={(keyvalue) => onKeyValueAttributeChange(keyvalue, selectedType)}
          keyPlaceholder={'value'}
          valuePlaceholder={'label'}
        />
      );
    } else if (selectedType === SupportedType.IDLabelPair) {
      return (
        <KeyValuePairs
          keyValues={localValues}
          onKeyValueChange={(keyvalue) => onKeyValueAttributeChange(keyvalue, selectedType)}
          keyPlaceholder={'id'}
          valuePlaceholder={'label'}
        />
      );
    } else if (selectedType === SupportedType.Number) {
      // TODO ask if there is a dsv for number inputs
      return (
        <PluginInput
          widgetType={'dsv'}
          value={localValues}
          setValue={onAttributeChange}
          label={field}
          required={true}
        />
      );
    } else {
      return (
        <PluginInput
          widgetType={'dsv'}
          value={localValues}
          setValue={onAttributeChange}
          label={field}
          required={true}
        />
      );
    }
  };

  return (
    <div>
      <If condition={cleanSupportedTypes.length > 1}>
        <div className={classes.typeSelectInput}>
          <PluginInput
            widgetType={'select'}
            value={selectedType}
            setValue={switchInputType}
            label={'select value type'}
            options={cleanSupportedTypes}
          />
        </div>
      </If>

      {renderAttributeMultipleValuesInput()}
    </div>
  );
};

const AttributeMultipleValuesInput = withStyles(styles)(AttributeMultipleValuesInput);
export default AttributeMultipleValuesInput;
