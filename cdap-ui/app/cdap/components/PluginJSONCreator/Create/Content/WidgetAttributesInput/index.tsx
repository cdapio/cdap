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

import IconButton from '@material-ui/core/IconButton';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import AddIcon from '@material-ui/icons/Add';
import DeleteIcon from '@material-ui/icons/Delete';
import If from 'components/If';
import PluginTextboxInput from 'components/PluginJSONCreator/Create/Content/PluginTextboxInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetInput: {
      '& > *': {
        width: '80%',
        marginTop: '10px',
        marginBottom: '10px',
      },
    },
  };
};

export const WidgetTypeToAttribues = {
  keyvalue: {
    'key-placeholder': 'string',
    'value-placeholder': 'string',
    'kv-delimiter': 'string',
    delimiter: 'string',
  },
};

const WidgetAttributesInputView: React.FC<WithStyles<typeof styles>> = ({
  classes,
  widgetID,
  widgetToInfo,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  function onAttributeChange(attributeField) {
    return (newAttriuteVal) => {
      setWidgetToAttributes((prevObjs) => ({
        ...prevObjs,
        [widgetID]: { ...prevObjs[widgetID], [attributeField]: newAttriuteVal },
      }));
    };
  }

  function onAddWidgetAttribute(attributeIndex) {}

  function onDeleteWidgetAttribute(attributeIndex) {}

  const widget = widgetToInfo[widgetID];
  const widgetType = widget ? widget.widgetType : null;
  const attributesInterface = widgetType ? WidgetTypeToAttribues[widgetType] : {};
  const attributeFields = attributesInterface ? Object.keys(attributesInterface) : [];
  return (
    <If condition={widget && widget.widgetType}>
      <div>
        {attributeFields.map((attributeField, i) => {
          return (
            <div>
              <PluginTextboxInput
                value={widgetToAttributes[widgetID][attributeField]}
                setValue={onAttributeChange(attributeField)}
                label={attributeField}
              />
              <IconButton onClick={() => onAddWidgetAttribute(i)} data-cy="add-row">
                <AddIcon fontSize="small" />
              </IconButton>
              <IconButton
                onClick={() => onDeleteWidgetAttribute(i)}
                color="secondary"
                data-cy="remove-row"
              >
                <DeleteIcon fontSize="small" />
              </IconButton>
            </div>
          );
        })}
      </div>
    </If>
  );
};
/*(prevProps, nextProps) => {
    const result =
      prevProps.widgetObject.label === nextProps.widgetObject.label &&
      prevProps.widgetObject.name === nextProps.widgetObject.name &&
      prevProps.widgetObject.widgetType == nextProps.widgetObject.widgetType;
    return result;
  }*/

const WidgetAttributesInput = withStyles(styles)(WidgetAttributesInputView);
export default WidgetAttributesInput;
