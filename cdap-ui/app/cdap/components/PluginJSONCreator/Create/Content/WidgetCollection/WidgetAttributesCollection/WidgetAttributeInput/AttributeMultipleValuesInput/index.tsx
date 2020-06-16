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

/*
 * AttributeMultipleValuesInput is a component used for setting widget-attributes of "array" type.
 * For instance, users will be able to set following widget attributes.
 *
 * Example 1)
 *     "options": [
 *       {
 *         "id": "true",
 *        "label": "true"
 *       },
 *       {
 *         "id": "false",
 *         "label": "false"
 *       }
 *     ]
 * Example 2)
 *     "values": [
 *       "json",
 *       "xml",
 *       "tsv",
 *       "csv",
 *       "text",
 *       "blob"
 *     ],
 *
 * As we can see from the examples above, this can be an array of string or an array of object (ID-Label pair).
 * In this component, a prop named 'supportedTypes' stores all the available types for a certain field.
 *
 * For instance, 'select' widget has an attribute field called 'options'.
 * 'options' can be of different types, therefore its 'supportedTypes' is as following:
 *     ['ISelectOptions[]', 'string[]', 'number[]']
 * If the user selects ISelectOptions[], then it that means the 'options' field will be
 * an array of ISelectOptions.
 * Since ISelectOptions interface is defined to be a 'value' and 'label' pair,
 * our component should render multiple input rows of key-value pairs.
 *
 * Some types such as 'IDropdownOption', can be string, number, or value-label pair.
 *     export type IDropdownOption = string | number | IComplexDropdown;
 *     interface IComplexDropdown {
 *       value: string | number;
 *       label: string;
 *     }
 * Therefore, we should futher process IDropdownOption[], by adding all the available types in the function
 * 'processSupportedTypes'.
 *     supportedTypes.add(SupportedType.String);
 *     supportedTypes.add(SupportedType.Number);
 *     supportedTypes.add(SupportedType.ValueLabelPair);
 */

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
        // Add the default type in case the type is not supported
        allTypes.add(SupportedType.String);
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
    setCurrentInput(getCurrentWidgetAttributeValues(initialType));
  }, []);

  /*
   * The input fields can have some pre-populated values.
   * For instance, when the user imports a plugin JSON file into the UI,
   * It should parse those pre-populated values of widget attributes
   * and decide what should be 'selectedType' for the component.
   */
  function getInitialType() {
    const widgetAttributeValues = widgetToAttributes[widgetID][field];
    let newType;
    if (!widgetAttributeValues || widgetAttributeValues.length === 0) {
      newType = cleanSupportedTypes[0];
    } else if (widgetAttributeValues[0].value) {
      newType = SupportedType.ValueLabelPair;
    } else if (widgetAttributeValues[0].id) {
      newType = SupportedType.IDLabelPair;
    } else {
      newType = SupportedType.String;
    }
    return newType;
  }

  /*
   * The input fields can have some pre-populated values.
   * For instance, when the user imports a plugin JSON file into the UI,
   * it should parse those pre-populated values of widget attributes
   * and populate the input fields.
   */
  function getCurrentWidgetAttributeValues(newType) {
    if (!newType) {
      return '';
    } else if (newType === SupportedType.Number || newType === SupportedType.String) {
      return processAttributeValues();
    } else {
      return processKeyValueAttributeValues();
    }
  }

  /*
   * Process simple attribute values in order to pass them to the input component.
   * The component 'CSVWidget' receives the data of following structure.
   *
   * Example)
   *     "GET,POST,PUT,DELETE"
   */
  function processAttributeValues() {
    const widgetAttributeValues = widgetToAttributes[widgetID][field];
    return widgetAttributeValues ? widgetAttributeValues.join(COMMON_DELIMITER) : '';
  }

  /*
   * Process key-value attribute values in order to pass the values to the input component.
   * The component 'KeyValuePairs' receives the data of following structure.
   *    { pairs: [{ key: '', value: '' }] }
   */
  function processKeyValueAttributeValues() {
    const widgetAttributeValues = widgetToAttributes[widgetID][field];
    if (widgetAttributeValues && widgetAttributeValues.length > 0) {
      return {
        pairs: widgetAttributeValues.map((keyvalue) => {
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

  const switchInputType = (newType) => {
    setSelectedType(newType);
    setCurrentInput(getCurrentWidgetAttributeValues(newType));

    // When user switches the 'selectedType', the value will reset back to an empty array.
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

      <If condition={currentInput}>{renderAttributeMultipleValuesInput()}</If>
    </div>
  );
};

const AttributeMultipleValuesInput = withStyles(styles)(AttributeMultipleValuesInputView);
export default AttributeMultipleValuesInput;
