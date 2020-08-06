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

import * as React from 'react';

import { COMMON_DELIMITER } from 'components/PluginJSONCreator/constants';
import { List } from 'immutable';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';

const AttributeRowsInput = ({
  widgetID,
  field,
  selectedType,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  const [currentAttributeValues, setCurrentAttributeValues] = React.useState('');

  React.useEffect(() => {
    if (!localWidgetToAttributes || !localWidgetToAttributes.get(widgetID)) {
      setCurrentAttributeValues('');
    } else {
      const existingAttributeValues = localWidgetToAttributes.get(widgetID).get(field);
      if (existingAttributeValues) {
        setCurrentAttributeValues(processAttributeValues(existingAttributeValues));
      }
    }
  }, [selectedType, localWidgetToAttributes]);

  /*
   * The input fields can have some pre-populated values.
   * For instance, when the user imports a plugin JSON file into the UI,
   * it should parse those pre-populated values of widget attributes
   * and populate the input fields.
   *
   * Process attribute values to pass the values to the input fields.
   * The component 'CSVWidget' receives the data of following structure.
   *
   * Example)
   *     "GET,POST,PUT,DELETE"
   */
  function processAttributeValues(attributeValues) {
    return attributeValues ? attributeValues.join(COMMON_DELIMITER) : '';
  }

  const onAttributeChange = (newVal) => {
    setCurrentAttributeValues(newVal);
    setLocalWidgetToAttributes(
      localWidgetToAttributes.setIn([widgetID, field], List(newVal.split(COMMON_DELIMITER)))
    );
  };

  return (
    <PluginInput
      widgetType={'dsv'}
      value={currentAttributeValues}
      onChange={onAttributeChange}
      label={field}
      required={true}
    />
  );
};

export default AttributeRowsInput;
