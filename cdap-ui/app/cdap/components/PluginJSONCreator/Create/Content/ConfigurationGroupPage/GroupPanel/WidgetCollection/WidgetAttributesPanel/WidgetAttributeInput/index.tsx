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

import MultipleAttributesInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/MultipleAttributesInput';
import SingleAttributeInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput/SingleAttributeInput';
import { WIDGET_FACTORY } from 'components/AbstractWidget/AbstractWidgetFactory';

const styles = (theme): StyleRules => {
  return {
    widgetAttributeInput: {
      width: '100%',
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(3),
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
   *     1. WIDGET_FACTORY = {
   *       'connection-browser': ConnectionBrowser
   *     }
   *     2. ConnectionBrowser.getWidgetAttributes = () => {
   *          return {
   *            connectionType: { type: 'string', required: true },
   *            label: { type: 'string', required: true },
   *          };
   *       };
   *     3. widgetType = 'connection-browser'
   *     4. field = 'connectionType'
   *
   * In this case, fieldInfo = { type: 'string', required: true }.
   * Therefore, we will render out the input that matches this requirement.
   */
  let fieldInfo;
  try {
    const comp = WIDGET_FACTORY[widgetType];
    const widgetAttributes = comp.getWidgetAttributes();
    fieldInfo = widgetAttributes[field];
  } catch (e) {
    fieldInfo = { type: 'string', required: false };
  }

  const renderAttributeInput = () => {
    if (!fieldInfo) {
      return;
    }

    // widget-attributes is considered as multiple attributes in following cases:
    // e.g. fieldInfo.type = 'string[] | number[]'
    // e.g. fieldInfo.type = 'Record<string, string>'
    const isMultipleInput = fieldInfo.type.includes('[]') || fieldInfo.type.includes('Record');
    const supportedTypes = fieldInfo.type.split('|').map((item) => item.trim());

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
