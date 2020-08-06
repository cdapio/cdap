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

import { WIDGET_CATEGORY, WIDGET_TYPES } from 'components/PluginJSONCreator/constants';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import { Map } from 'immutable';
import PluginInput from 'components/PluginJSONCreator/Create/Content/PluginInput';
import { WIDGET_FACTORY } from 'components/AbstractWidget/AbstractWidgetFactory';
import { useWidgetState } from 'components/PluginJSONCreator/Create';

const styles = (theme): StyleRules => {
  return {
    widgetInputContainer: {
      width: '100%',
    },
    widgetInput: {
      width: '100%',
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(3),
    },
  };
};

interface IWidgetInfoInputProps extends WithStyles<typeof styles> {
  widgetID: string;
}

const WidgetInfoInputView: React.FC<IWidgetInfoInputProps> = ({ classes, widgetID }) => {
  const { widgetInfo, setWidgetInfo, widgetToAttributes, setWidgetToAttributes } = useWidgetState();

  function onNameChange() {
    return (name) => {
      setWidgetInfo(widgetInfo.setIn([widgetID, 'name'], name));
    };
  }

  function onLabelChange() {
    return (label) => {
      setWidgetInfo(widgetInfo.setIn([widgetID, 'label'], label));
    };
  }

  function onWidgetCategoryChange() {
    return (widgetCategory) => {
      setWidgetInfo(widgetInfo.setIn([widgetID, 'widgetCategory'], widgetCategory));
    };
  }

  function onWidgetTypeChange() {
    return (widgetType) => {
      setWidgetInfo(widgetInfo.setIn([widgetID, 'widgetType'], widgetType));

      // Change widget-attributes so that it corresponds to the widget-type
      const comp = WIDGET_FACTORY[widgetType];
      const widgetAttributes = comp.getWidgetAttributes();
      setWidgetToAttributes(
        widgetToAttributes.set(
          widgetID,
          Map(
            Object.keys(widgetAttributes).reduce((acc, curr) => {
              acc[curr] = undefined;
              return acc;
            }, {})
          )
        )
      );
    };
  }

  const widget = widgetInfo.get(widgetID);
  return React.useMemo(
    () => (
      <div className={classes.widgetInputContainer}>
        <div className={classes.widgetInput}>
          <PluginInput
            widgetType={'textbox'}
            value={widget.get('name')}
            onChange={onNameChange()}
            label={'Name'}
            placeholder={'Name a Widget'}
            required={false}
          />
        </div>
        <div className={classes.widgetInput}>
          <PluginInput
            widgetType={'textbox'}
            value={widget.get('label')}
            onChange={onLabelChange()}
            label={'Label'}
            placeholder={'Label a Widget'}
            required={false}
          />
        </div>
        <div className={classes.widgetInput}>
          <PluginInput
            widgetType={'select'}
            value={widget.get('widgetCategory')}
            onChange={onWidgetCategoryChange()}
            label={'Category'}
            options={WIDGET_CATEGORY}
            required={false}
          />
        </div>
        <div className={classes.widgetInput}>
          <PluginInput
            widgetType={'select'}
            value={widget.get('widgetType')}
            onChange={onWidgetTypeChange()}
            label={'Widget type'}
            options={WIDGET_TYPES}
            required={true}
          />
        </div>
      </div>
    ),
    [widgetInfo]
  );
};

const WidgetInfoInput = withStyles(styles)(WidgetInfoInputView);
export default WidgetInfoInput;
