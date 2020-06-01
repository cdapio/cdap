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

<<<<<<< HEAD
import Dialog from '@material-ui/core/Dialog';
import DialogContent from '@material-ui/core/DialogContent';
import DialogTitle from '@material-ui/core/DialogTitle';
import IconButton from '@material-ui/core/IconButton';
=======
import { Dialog, DialogContent, DialogTitle, IconButton } from '@material-ui/core';
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import CloseIcon from '@material-ui/icons/Close';
import If from 'components/If';
import { h2Styles } from 'components/Markdown/MarkdownHeading';
import { WIDGET_TYPE_TO_ATTRIBUTES } from 'components/PluginJSONCreator/constants';
import WidgetAttributeInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection/WidgetAttributeInput';
import WidgetInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetInput';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';

const styles = (theme): StyleRules => {
  return {
<<<<<<< HEAD
=======
    widgetAttributeInput: {
      '& > *': {
        width: '100%',
        marginTop: '10px',
        marginBottom: '10px',
      },
    },
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
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

interface IWidgetAttributesCollectionProps extends WithStyles<typeof styles>, ICreateContext {
  widgetAttributesOpen: boolean;
  onWidgetAttributesClose: () => void;
  widgetID: number;
}

const WidgetAttributesCollectionView: React.FC<IWidgetAttributesCollectionProps> = ({
  classes,
  widgetAttributesOpen,
  onWidgetAttributesClose,
  widgetID,
<<<<<<< HEAD
  widgetInfo,
  setWidgetInfo,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  const widget = widgetInfo[widgetID];
=======
  widgetToInfo,
  setWidgetToInfo,
  widgetToAttributes,
  setWidgetToAttributes,
}) => {
  const widget = widgetToInfo[widgetID];
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
  const widgetType = widget ? widget.widgetType : null;
  const attributeFields =
    widgetToAttributes && widgetToAttributes[widgetID]
      ? Object.keys(widgetToAttributes[widgetID])
      : [];
  return (
    <div>
      <Dialog
        open={widgetAttributesOpen}
        onClose={onWidgetAttributesClose}
        disableBackdropClick={true}
        fullWidth={true}
        maxWidth={'md'}
        classes={{ paper: classes.attributeDialog }}
      >
        <DialogTitle disableTypography className={classes.dialogTitle}>
          <IconButton onClick={onWidgetAttributesClose}>
            <CloseIcon />
          </IconButton>
        </DialogTitle>
        <DialogContent>
          <div className={classes.widgetAttributesTitle}>
            <h1 className={classes.h2Title}>Widget Property</h1>
          </div>
          <WidgetInput
<<<<<<< HEAD
            widgetInfo={widgetInfo}
            widgetID={widgetID}
            setWidgetInfo={setWidgetInfo}
=======
            widgetToInfo={widgetToInfo}
            widgetID={widgetID}
            setWidgetToInfo={setWidgetToInfo}
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
            widgetToAttributes={widgetToAttributes}
            setWidgetToAttributes={setWidgetToAttributes}
          />
          <If condition={attributeFields && attributeFields.length > 0}>
            <div className={classes.widgetAttributesTitle}>
              <h2 className={classes.h2Title}>Configure Widget</h2>
            </div>
          </If>
          {attributeFields.map((field, fieldIndex) => {
            const fieldInfo = WIDGET_TYPE_TO_ATTRIBUTES[widgetType]
              ? WIDGET_TYPE_TO_ATTRIBUTES[widgetType][field]
              : {};
            return (
<<<<<<< HEAD
              <WidgetAttributeInput
                key={fieldIndex}
                widgetType={widgetType}
                field={field}
                fieldInfo={fieldInfo}
                widgetToAttributes={widgetToAttributes}
                setWidgetToAttributes={setWidgetToAttributes}
                widgetID={widgetID}
              />
=======
              <div className={classes.widgetAttributeInput}>
                <WidgetAttributeInput
                  widgetType={widgetType}
                  field={field}
                  fieldInfo={fieldInfo}
                  widgetToAttributes={widgetToAttributes}
                  setWidgetToAttributes={setWidgetToAttributes}
                  widgetID={widgetID}
                />
              </div>
>>>>>>> f1bba4bdece... [CDAP-16871] Configure widget-attributes for each property (plugin JSON creator)
            );
          })}
        </DialogContent>
      </Dialog>
    </div>
  );
};

const WidgetAttributesCollection = withStyles(styles)(WidgetAttributesCollectionView);
export default WidgetAttributesCollection;
