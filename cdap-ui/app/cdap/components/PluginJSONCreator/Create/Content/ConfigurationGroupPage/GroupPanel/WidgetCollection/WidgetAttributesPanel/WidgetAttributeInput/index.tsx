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
import { WIDGET_TYPE_TO_ATTRIBUTES } from 'components/PluginJSONCreator/constants';
import MultipleAttributesInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/MultipleAttributesInput';
import SingleAttributeInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/SingleAttributeInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetAttributeInput: {
      width: '100%',
      marginTop: '20px',
      marginBottom: '20px',
    },
  };
};

const WidgetAttributeInputView = ({
  classes,
  field,
  widgetID,
  widgetType,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  /*
   * Let's suppose the following condition.
   *     1. WIDGET_TYPE_TO_ATTRIBUTES = {
   *       'connection-browser': {
   *         connectionType: { type: 'string', required: true },
   *         label: { type: 'string', required: true },
   *       }
   *     }
   *     2. widgetType = 'connection-browser'
   *     3. field = 'connectionType'
   *
   *
   * In this case, fieldInfo = { type: 'string', required: true }.
   * Therefore, we will render out the input that matches this requirement.
   */
  const fieldInfo = WIDGET_TYPE_TO_ATTRIBUTES[widgetType]
    ? WIDGET_TYPE_TO_ATTRIBUTES[widgetType][field]
    : {};

  const renderAttributeInput = () => {
    if (!fieldInfo) {
      return;
    }

    const isMultipleInput = fieldInfo.type.includes('[]');
    const supportedTypes = fieldInfo.type.split('|');
    if (isMultipleInput) {
      return (
        <MultipleAttributesInput
          widgetID={widgetID}
          field={field}
          supportedTypes={supportedTypes}
          localWidgetToAttributes={localWidgetToAttributes}
          setLocalWidgetToAttributes={setLocalWidgetToAttributes}
        />
      );
    } else {
      return (
        <SingleAttributeInput
          widgetID={widgetID}
          field={field}
          widgetType={widgetType}
          fieldInfo={fieldInfo}
          localWidgetToAttributes={localWidgetToAttributes}
          setLocalWidgetToAttributes={setLocalWidgetToAttributes}
        />
      );
    }
  };

  return <div className={classes.widgetAttributeInput}>{renderAttributeInput()}</div>;
};

const WidgetAttributeInput = withStyles(styles)(WidgetAttributeInputView);
export default WidgetAttributeInput;
