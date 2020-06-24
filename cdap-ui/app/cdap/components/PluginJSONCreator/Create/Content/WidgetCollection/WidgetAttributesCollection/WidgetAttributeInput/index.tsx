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
import MultipleAttributesInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/MultipleAttributesInput';
import SingleAttributeInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput/SingleAttributeInput';
import * as React from 'react';

const styles = (): StyleRules => {
  return {
    widgetAttributeInput: {
      width: '100%',
      marginTop: '10px',
      marginBottom: '10px',
    },
  };
};

const WidgetAttributeInputView = ({
  classes,
  field,
  fieldInfo,
  widgetID,
  widgetType,
  localWidgetToAttributes,
  setLocalWidgetToAttributes,
}) => {
  const renderAttributeInput = () => {
    if (!fieldInfo) {
      return;
    }

    const isMultipleInput = fieldInfo.type.includes('[]');
    const supportedTypes = fieldInfo.type.split('|');
    if (isMultipleInput) {
      return (
        <MultipleAttributesInput
          supportedTypes={supportedTypes}
          widgetID={widgetID}
          field={field}
          localWidgetToAttributes={localWidgetToAttributes}
          setLocalWidgetToAttributes={setLocalWidgetToAttributes}
        />
      );
    } else {
      return (
        <SingleAttributeInput
          widgetID={widgetID}
          widgetType={widgetType}
          field={field}
          fieldInfo={fieldInfo}
          localWidgetToAttributes={localWidgetToAttributes}
          setLocalWidgetToAttributes={setLocalWidgetToAttributes}
        />
      );
    }
  };

  return <div className={classes.widgetAttributeINput}>{renderAttributeInput()}</div>;
};

const WidgetAttributeInput = withStyles(styles)(WidgetAttributeInputView);
export default WidgetAttributeInput;
