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

import Button from '@material-ui/core/Button';
import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import IconButton from '@material-ui/core/IconButton';
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import CloseIcon from '@material-ui/icons/Close';
import If from 'components/If';
import { h2Styles } from 'components/Markdown/MarkdownHeading';
import { WIDGET_TYPE_TO_ATTRIBUTES } from 'components/PluginJSONCreator/constants';
import { useWidgetState } from 'components/PluginJSONCreator/Create';
import WidgetAttributeInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetAttributesPanel/WidgetAttributeInput';
import WidgetInfoInput from 'components/PluginJSONCreator/Create/Content/ConfigurationGroupPage/GroupPanel/WidgetCollection/WidgetPanel/WidgetInfoInput';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
    widgetAttributesTitle: {
      marginTop: '15px',
      marginBottom: '15px',
    },
    h2Title: {
      ...h2Styles(theme).root,
      marginBottom: '5px',
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

  // If 'widgetToAttributes' changes, local values should also be updated.
  // For instance, if user changes the widgetType.
  React.useEffect(() => {
    setLocalWidgetToAttributes(widgetToAttributes);
  }, [widgetToAttributes]);

  // Check whether local changes to widget attributes is saved
  const [localSaved, setLocalSaved] = React.useState(true);

  const widgetType = widgetInfo.get(widgetID).get('widgetType') || '';

  // There are situations when the widgets from imported file do not include
  // all the required 'widget-atttributes'. Therefore, this approach will include
  // those missing fields.
  const attributeFields = WIDGET_TYPE_TO_ATTRIBUTES[widgetType]
    ? Object.keys(WIDGET_TYPE_TO_ATTRIBUTES[widgetType])
    : [];

  // When local changes to widget attributes happen
  React.useEffect(() => {
    setLocalSaved(false);
  }, [localWidgetToAttributes]);

  function saveWidgetToAttributes() {
    return () => {
      const localAttributeValues = localWidgetToAttributes.get(widgetID);
      setWidgetToAttributes(widgetToAttributes.set(widgetID, localAttributeValues));
      setLocalSaved(true);
    };
  }

  return React.useMemo(
    () => (
      <If condition={widgetAttributesOpen}>
        <Dialog
          open={true}
          onClose={closeWidgetAttributes}
          disableBackdropClick={true}
          fullWidth={true}
          maxWidth={'md'}
          classes={{ paper: classes.attributeDialog }}
        >
          <DialogTitle disableTypography className={classes.dialogTitle}>
            <IconButton onClick={closeWidgetAttributes}>
              <CloseIcon />
            </IconButton>
          </DialogTitle>
          <DialogContent>
            <div className={classes.widgetAttributesTitle}>
              <h1 className={classes.h2Title}>Widget Property</h1>
            </div>
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

            <Button
              variant="contained"
              color="primary"
              disabled={localSaved}
              onClick={saveWidgetToAttributes()}
            >
              Save
            </Button>
          </DialogContent>
        </Dialog>
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
