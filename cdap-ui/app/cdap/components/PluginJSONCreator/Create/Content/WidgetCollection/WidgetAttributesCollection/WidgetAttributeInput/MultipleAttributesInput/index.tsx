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
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import KeyvalueRowsInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/MultipleAttributesInput/KeyvalueRowsInput';
import RowsInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/MultipleAttributesInput/RowsInput';
import { fromJS, Map } from 'immutable';
import * as React from 'react';

/*
 * MultipleAttributesInput is a component used for setting widget-attributes of "array" type.
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
        break;
    }
  });
  return allTypes;
};

const MultipleAttributesInputView = ({
  classes,
  supportedTypes,
  widgetID,
  field,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  const cleanSupportedTypes = Array.from(processSupportedTypes(supportedTypes).values());
  const [selectedType, setSelectedType] = React.useState(null);

  // On initial load, set 'selectedType' based on the existing widgetToAttributes values.
  React.useEffect(() => {
    const initialType = getInitialType();
    setSelectedType(initialType);
  }, []);

  /*
   * The input fields can have some pre-populated values.
   * For instance, when the user imports a plugin JSON file into the UI,
   * It should parse those pre-populated values of widget attributes
   * and decide what should be 'selectedType' for the component.
   */
  function getInitialType() {
    const existingAttributeValues = localWidgetToAttributes.get(widgetID).get(field);
    let newType;
    if (!existingAttributeValues || existingAttributeValues.size === 0) {
      newType = cleanSupportedTypes[0];
    } else if (Map.isMap(existingAttributeValues.get(0))) {
      newType = existingAttributeValues.get(0).get('value')
        ? SupportedType.ValueLabelPair
        : SupportedType.IDLabelPair;
    } else {
      newType = SupportedType.String;
    }
    return newType;
  }

  const switchSelectedType = (newType) => {
    setSelectedType(newType);
    // When user switches the 'selectedType', attribute values should reset to an empty array.
    setLocalWidgetToAttributes(
      fromJS(localWidgetToAttributes).setIn([widgetID, field], fromJS([]))
    );
  };

  return (
    <div>
      <If condition={cleanSupportedTypes.length > 1}>
        <div className={classes.typeSelectInput}>
          <PluginInput
            widgetType={'select'}
            value={selectedType}
            onChange={switchSelectedType}
            label={'select value type'}
            options={cleanSupportedTypes}
          />
        </div>
      </If>

      <If
        condition={
          selectedType === SupportedType.IDLabelPair ||
          selectedType === SupportedType.ValueLabelPair
        }
      >
        <KeyvalueRowsInput
          widgetID={widgetID}
          field={field}
          selectedType={selectedType}
          localWidgetToAttributes={localWidgetToAttributes}
          setLocalWidgetToAttributes={setLocalWidgetToAttributes}
        />
      </If>
      <If
        condition={selectedType === SupportedType.String || selectedType === SupportedType.Number}
      >
        <RowsInput
          widgetID={widgetID}
          field={field}
          selectedType={selectedType}
          localWidgetToAttributes={localWidgetToAttributes}
          setLocalWidgetToAttributes={setLocalWidgetToAttributes}
        />
      </If>
    </div>
  );
};

const MultipleAttributesInput = withStyles(styles)(MultipleAttributesInputView);
export default MultipleAttributesInput;
