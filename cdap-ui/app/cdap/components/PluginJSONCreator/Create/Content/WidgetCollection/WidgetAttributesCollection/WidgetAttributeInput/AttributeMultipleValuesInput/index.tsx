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
      marginTop: '5px',
      marginBottom: '5px',
      width: '20%',
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

const AttributeMultipleValuesInputView = ({
  classes,
  supportedTypes,
  widgetToAttributes,
  setWidgetToAttributes,
  widgetID,
  field,
}) => {
  const cleanSupportedTypes = Array.from(processSupportedTypes(supportedTypes).values());
  const [selectedType, setSelectedType] = React.useState(null);
  const [currentInput, setCurrentInput] = React.useState(null);

  React.useEffect(() => {
    const initialType = getInitialType();
    setSelectedType(initialType);
    setCurrentInput(getUpdatedCurrentInput(initialType));
  }, []);

  function getInitialType() {
    const widgetAttributes = widgetToAttributes[widgetID][field];
    let newType;
    if (!widgetAttributes || widgetAttributes.length === 0) {
      newType = cleanSupportedTypes[0];
    } else if (widgetAttributes[0].value) {
      newType = SupportedType.ValueLabelPair;
    } else if (widgetAttributes[0].id) {
      newType = SupportedType.IDLabelPair;
    } else {
      newType = SupportedType.String;
    }
    return newType;
  }

  function getUpdatedCurrentInput(newType) {
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

  const switchInputType = (newType) => {
    setSelectedType(newType);
    setCurrentInput(getUpdatedCurrentInput(newType));
    setWidgetToAttributes((prevObjs) => ({
      ...prevObjs,
      [widgetID]: { ...prevObjs[widgetID], [field]: [] },
    }));
  };

  const onAttributeChange = (newVal) => {
    setCurrentInput(newVal);
    setWidgetToAttributes((prevObjs) => ({
      ...prevObjs,
      [widgetID]: { ...prevObjs[widgetID], [field]: newVal.split(COMMON_DELIMITER) },
    }));
  };

  const onKeyValueAttributeChange = (keyvalue, type: SupportedType) => {
    if (type === SupportedType.ValueLabelPair) {
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
    switch (selectedType) {
      case SupportedType.ValueLabelPair:
        return (
          <KeyValuePairs
            keyValues={currentInput}
            onKeyValueChange={(keyvalue) => onKeyValueAttributeChange(keyvalue, selectedType)}
            keyPlaceholder={'value'}
            valuePlaceholder={'label'}
          />
        );
      case SupportedType.IDLabelPair:
        return (
          <KeyValuePairs
            keyValues={currentInput}
            onKeyValueChange={(keyvalue) => onKeyValueAttributeChange(keyvalue, selectedType)}
            keyPlaceholder={'id'}
            valuePlaceholder={'label'}
          />
        );
      case SupportedType.Number:
        return (
          <PluginInput
            widgetType={'dsv'}
            value={currentInput}
            onChange={onAttributeChange}
            label={field}
            required={true}
          />
        );
      default:
        return (
          <PluginInput
            widgetType={'dsv'}
            value={currentInput}
            onChange={onAttributeChange}
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
            onChange={switchInputType}
            label={'select value type'}
            options={cleanSupportedTypes}
          />
        </div>
      </If>

      {renderAttributeMultipleValuesInput()}
    </div>
  );
};

const AttributeMultipleValuesInput = withStyles(styles)(AttributeMultipleValuesInputView);
export default AttributeMultipleValuesInput;
