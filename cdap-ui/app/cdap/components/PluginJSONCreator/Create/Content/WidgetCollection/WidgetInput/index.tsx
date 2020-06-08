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
import withStyles, { StyleRules, WithStyles } from '@material-ui/core/styles/withStyles';
import WidgetActionButtons from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetActionButtons';
import WidgetAttributesCollection from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetAttributesCollection';
import WidgetInfoInput from 'components/PluginJSONCreator/Create/Content/WidgetCollection/WidgetInfoInput';
import { ICreateContext } from 'components/PluginJSONCreator/CreateContextConnect';
import * as React from 'react';
import { Draggable } from 'react-beautiful-dnd';

const styles = (): StyleRules => {
  return {
    eachWidget: {
      display: 'grid',
      gridTemplateColumns: '5fr 1fr',
      marginLeft: 'auto',
      marginRight: 'auto',
    },
  };
};

interface IWidgetInputProps extends WithStyles<typeof styles>, ICreateContext {
  index: number;
  widgetID: string;
  widgetAttributesOpen: boolean;
  addWidgetToGroup: () => void;
  deleteWidgetFromGroup: () => void;
  openWidgetAttributes: () => void;
  closeWidgetAttributes: () => void;
}

const WidgetInputView: React.FC<IWidgetInputProps> = ({
  classes,
  index,
  widgetID,
  widgetInfo,
  setWidgetInfo,
  widgetToAttributes,
  setWidgetToAttributes,
  widgetAttributesOpen,
  addWidgetToGroup,
  deleteWidgetFromGroup,
  openWidgetAttributes,
  closeWidgetAttributes,
}) => {
  return (
    <Draggable key={widgetID} draggableId={widgetID} index={index}>
      {(provided) => (
        <div
          className={classes.eachWidget}
          ref={provided.innerRef}
          {...provided.draggableProps}
          {...provided.dragHandleProps}
        >
          <WidgetInfoInput
            widgetInfo={widgetInfo}
            widgetID={widgetID}
            setWidgetInfo={setWidgetInfo}
            widgetToAttributes={widgetToAttributes}
            setWidgetToAttributes={setWidgetToAttributes}
          />
          <WidgetActionButtons
            onAddWidgetToGroup={addWidgetToGroup}
            onDeleteWidgetFromGroup={deleteWidgetFromGroup}
          />

          <WidgetAttributesCollection
            widgetAttributesOpen={widgetAttributesOpen}
            onWidgetAttributesClose={closeWidgetAttributes}
            widgetID={widgetID}
            widgetInfo={widgetInfo}
            setWidgetInfo={setWidgetInfo}
            widgetToAttributes={widgetToAttributes}
            setWidgetToAttributes={setWidgetToAttributes}
          />
          <Button
            variant="contained"
            color="primary"
            component="span"
            onClick={openWidgetAttributes}
          >
            Attributes
          </Button>
          {provided.placeholder}
        </div>
      )}
    </Draggable>
  );
};

const WidgetInput = withStyles(styles)(WidgetInputView);
export default WidgetInput;
