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

import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';

import Button from '@material-ui/core/Button';
import If from 'components/If';
import StandardModal from 'components/StandardModal';
import { WIDGET_TYPE_TO_ATTRIBUTES } from 'components/PluginJSONCreator/constants';
import WidgetAttributeInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput';
import WidgetInfoInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetPanel/WidgetInfoInput';
import { h2Styles } from 'components/Markdown/MarkdownHeading';
import { useWidgetState } from 'components/PluginJSONCreator/Create';

const styles = (theme): StyleRules => {
  return {
    widgetAttributesTitle: {
      marginTop: theme.spacing(3),
      marginBottom: theme.spacing(3),
    },
    h2Title: {
      ...h2Styles(theme).root,
      marginBottom: '5px',
    },
    setAttributes: {
      paddingLeft: theme.spacing(3),
      paddingRight: theme.spacing(3),
    },
    saveButton: {
      textTransform: 'none',
    },
    cancelButton: {
      textTransform: 'none',
    },
    actionButtons: {
      float: 'right',
    },
  };
};

interface IWidgetAttributesPanelProps extends WithStyles<typeof styles> {
  widgetID: string;
  widgetAttributesOpen: boolean;
  closeWidgetAttributes: () => void;
}

const WidgetAttributesPanelView: React.FC<IWidgetAttributesPanelProps> = ({
  classes,
  widgetID,
  widgetAttributesOpen,
  closeWidgetAttributes,
}) => {
  const { widgetInfo, widgetToAttributes, setWidgetToAttributes } = useWidgetState();

  // Keep local states of 'widgetToAttributes'.
  // 'widgetToAttributes' will only be changed when user clicks on 'save' button or closes the dialog.
  const [localWidgetToAttributes, setLocalWidgetToAttributes] = React.useState(widgetToAttributes);

  const widgetType = widgetInfo.get(widgetID).get('widgetType') || '';

  // There are situations when the widgets from imported file do not include
  // all the required 'widget-atttributes'. Therefore, this approach will include
  // those missing fields.
  const attributeFields = WIDGET_TYPE_TO_ATTRIBUTES[widgetType]
    ? Object.keys(WIDGET_TYPE_TO_ATTRIBUTES[widgetType])
    : [];

  function saveWidgetToAttributes() {
    return () => {
      const localAttributeValues = localWidgetToAttributes.get(widgetID);
      setWidgetToAttributes(widgetToAttributes.set(widgetID, localAttributeValues));
      closeWidgetAttributes();
    };
  }

  return React.useMemo(
    () => (
      <If condition={widgetAttributesOpen}>
        <StandardModal
          open={widgetAttributesOpen}
          toggle={closeWidgetAttributes}
          headerText={'Widget Attributes'}
        >
          <div className={classes.setAttributes}>
            <WidgetInfoInput widgetID={widgetID} />
            <If condition={attributeFields && attributeFields.length > 0}>
              <div className={classes.widgetAttributesTitle}>
                <h2 className={classes.h2Title}>Configure Widget</h2>
              </div>
            </If>
            {attributeFields.map((field, fieldIndex) => {
              return (
                <WidgetAttributeInput
                  key={fieldIndex}
                  widgetType={widgetType}
                  field={field}
                  widgetID={widgetID}
                  localWidgetToAttributes={localWidgetToAttributes}
                  setLocalWidgetToAttributes={setLocalWidgetToAttributes}
                />
              );
            })}

            <div className={classes.actionButtons}>
              <Button
                color="primary"
                onClick={() => closeWidgetAttributes()}
                className={classes.cancelButton}
              >
                Cancel
              </Button>

              <Button
                variant="contained"
                color="primary"
                onClick={saveWidgetToAttributes()}
                className={classes.saveButton}
              >
                Save
              </Button>
            </div>
          </div>
        </StandardModal>
      </If>
    ),
    [
      widgetAttributesOpen,
      widgetInfo.get(widgetID),
      widgetToAttributes.get(widgetID),
      localWidgetToAttributes.get(widgetID),
    ]
  );
};

const WidgetAttributesPanel = withStyles(styles)(WidgetAttributesPanelView);
export default WidgetAttributesPanel;
