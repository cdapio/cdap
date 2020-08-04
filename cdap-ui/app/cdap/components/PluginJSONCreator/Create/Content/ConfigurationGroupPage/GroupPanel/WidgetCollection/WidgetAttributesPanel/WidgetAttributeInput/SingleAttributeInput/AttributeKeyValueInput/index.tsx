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

import withStyles, { StyleRules } from '@material-ui/core/styles/withStyles';

import { Map } from 'immutable';
import WidgetWrapper from 'components/ConfigurationGroup/WidgetWrapper';

const styles = (theme): StyleRules => {
  return {
    widgetInput: {
      width: '100%',
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(3),
    },
  };
};

const AttributeKeyValueInputView = ({
  classes,
  keyField,
  valueField,
  keyRequired,
  valueRequired,
  widgetID,
  field,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  const onKeyChange = (newVal) => {
    const existingKeyvalue = localWidgetToAttributes.getIn([widgetID, field]);
    if (!Map.isMap(existingKeyvalue)) {
      setLocalWidgetToAttributes(
        localWidgetToAttributes.setIn([widgetID, field], Map({ [keyField]: '', [valueField]: '' }))
      );
    }
    setLocalWidgetToAttributes(localWidgetToAttributes.setIn([widgetID, field, keyField], newVal));
  };

  const onValueChange = (newVal) => {
    const existingKeyvalue = localWidgetToAttributes.getIn([widgetID, field]);
    if (!Map.isMap(existingKeyvalue)) {
      setLocalWidgetToAttributes(
        localWidgetToAttributes.setIn([widgetID, field], Map({ [keyField]: '', [valueField]: '' }))
      );
    }
    setLocalWidgetToAttributes(
      localWidgetToAttributes.setIn([widgetID, field, valueField], newVal)
    );
  };

  const keyWidget = {
    label: field + ' ' + keyField,
    name: keyField,
    'widget-type': 'textbox',
    'widget-attributes': {},
  };

  const valueWidget = {
    label: field + ' ' + valueField,
    name: keyField,
    'widget-type': 'textbox',
    'widget-attributes': {},
  };

  const keyProperty = {
    required: keyRequired,
    name: keyField,
  };

  const valueProperty = {
    required: valueRequired,
    name: valueField,
  };

  const widgetAttributeValues = localWidgetToAttributes.get(widgetID)
    ? localWidgetToAttributes.get(widgetID).get(field)
    : '';

  const currentKey = Map.isMap(widgetAttributeValues) ? widgetAttributeValues.get(keyField) : '';
  const currentValue = Map.isMap(widgetAttributeValues)
    ? widgetAttributeValues.get(valueField)
    : '';

  return (
    <div>
      <div className={classes.widgetInput}>
        <WidgetWrapper
          widgetProperty={keyWidget}
          pluginProperty={keyProperty}
          value={currentKey}
          onChange={onKeyChange}
        />
      </div>

      <div className={classes.widgetInput}>
        <WidgetWrapper
          widgetProperty={valueWidget}
          pluginProperty={valueProperty}
          value={currentValue}
          onChange={onValueChange}
        />
      </div>
    </div>
  );
};

const AttributeKeyValueInput = withStyles(styles)(AttributeKeyValueInputView);
export default AttributeKeyValueInput;
