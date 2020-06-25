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

import FormControl from '@material-ui/core/FormControl';
import InputLabel from '@material-ui/core/InputLabel';
import MenuItem from '@material-ui/core/MenuItem';
import Select from '@material-ui/core/Select';
import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';
import If from 'components/If';
import AttributeKeyvalueRowsInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/MultipleAttributesInput/AttributeKeyvalueRowsInput';
import AttributeRowsInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/MultipleAttributesInput/AttributeRowsInput';
import { List, Map } from 'immutable';
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

const styles = (theme): StyleRules => {
  return {
    multipleAtributesWrapper: {
      border: `1px solid`,
      borderColor: theme.palette.grey[300],
      borderRadius: '6px',
      position: 'relative',
      padding: '7px 10px 5px',
    },
    typeSelectInput: {
      marginTop: '5px',
      marginBottom: '5px',
      minWidth: '120',
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
    if (!existingAttributeValues || existingAttributeValues.isEmpty()) {
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

  const switchSelectedType = (e) => {
    setSelectedType(e.target.value);
    // When user switches the 'selectedType', attribute values should reset to an empty array.
    setLocalWidgetToAttributes(localWidgetToAttributes.setIn([widgetID, field], List([])));
  };

  return (
    <div className={classes.multipleAtributesWrapper}>
      <If condition={cleanSupportedTypes.length > 1}>
        <FormControl className={classes.typeSelectInput}>
          <InputLabel shrink>Data Type</InputLabel>
          <Select
            value={selectedType}
            onChange={switchSelectedType}
            className={classes.selectEmpty}
            displayEmpty={true}
          >
            {cleanSupportedTypes.map((type, index) => (
              <MenuItem key={index} value={type as string}>
                {type}
              </MenuItem>
            ))}
          </Select>
        </FormControl>
      </If>

      <If
        condition={
          selectedType === SupportedType.IDLabelPair ||
          selectedType === SupportedType.ValueLabelPair
        }
      >
        <AttributeKeyvalueRowsInput
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
        <AttributeRowsInput
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
