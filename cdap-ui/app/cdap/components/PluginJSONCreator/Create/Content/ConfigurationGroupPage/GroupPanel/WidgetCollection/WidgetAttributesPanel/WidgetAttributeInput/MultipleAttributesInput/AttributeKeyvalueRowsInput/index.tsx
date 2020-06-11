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

import { DELIMITER, KV_DELIMITER } from 'components/PluginJSONCreator/constants';
import { SupportedType } from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/MultipleAttributesInput';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { fromJS } from 'immutable';
import * as React from 'react';

const AttributeKeyvalueRowsInput = ({
  widgetID,
  field,
  selectedType,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  const [currentAttributeValues, setCurrentAttributeValues] = React.useState(null);

  React.useEffect(() => {
    setCurrentAttributeValues(processKeyValueAttributeValues());
  }, [selectedType, localWidgetToAttributes]);

  /*
   * The input fields can have some pre-populated values.
   * For instance, when the user imports a plugin JSON file into the UI,
   * it should parse those pre-populated values of widget attributes
   * and populate the input fields.
   *
   * Process attribute values to pass the values to the input fields.
   * The component 'KeyValueWidget' receives the data of following structure.
   *
   * Example)
   *     "key1;val1,key2;val2"
   */
  function processKeyValueAttributeValues() {
    const existingAttributeValues = localWidgetToAttributes.get(widgetID).get(field);
    if (!existingAttributeValues) {
      return '';
    }
    const keyvaluePairs = existingAttributeValues
      .map((keyvalue) => {
        if (keyvalue.get('id') !== undefined) {
          return [keyvalue.get('id'), keyvalue.get('label')].join(KV_DELIMITER);
        } else {
          return [keyvalue.get('value'), keyvalue.get('label')].join(KV_DELIMITER);
        }
      })
      .join(DELIMITER);
    return keyvaluePairs;
  }

  const onKeyValueAttributeChange = (keyvalue, type: SupportedType) => {
    const keyvaluePairs = keyvalue.split(DELIMITER).map((pair) => {
      const [key, value] = pair.split(KV_DELIMITER);
      if (type === SupportedType.ValueLabelPair) {
        return { value: key, label: value };
      } else {
        return { id: key, label: value };
      }
    });
    setLocalWidgetToAttributes(
      fromJS(localWidgetToAttributes).setIn([widgetID, field], fromJS(keyvaluePairs))
    );
  };

  return (
    <PluginInput
      widgetType={'keyvalue'}
      value={currentAttributeValues}
      onChange={(keyvalue) => onKeyValueAttributeChange(keyvalue, selectedType)}
      label={field}
      delimiter={DELIMITER}
      kvDelimiter={KV_DELIMITER}
      keyPlaceholder={selectedType === SupportedType.IDLabelPair ? 'id' : 'value'}
      valuePlaceholder={'label'}
    />
  );
};

export default AttributeKeyvalueRowsInput;
