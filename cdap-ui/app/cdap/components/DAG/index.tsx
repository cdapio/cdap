/*
 * Copyright © 2019 Cask Data, Inc.
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

import React from 'react';
import withStyles from '@material-ui/core/styles/withStyles';
import { DragDropContext, Droppable, Draggable } from 'react-beautiful-dnd';

const styles = (theme) => {
  return {
    root: {
      width: '100vw',
      height: 'calc(100vh - 104px)',
    },
    node: {
      height: '100px',
      width: '200px',
      border: '2px solid',
    },
    source: {
      borderColor: 'blue',
    },
    sink: {
      borderColor: 'pink',
    },
  };
};

function onDragEnd() {
  // TODO
  console.log(arguments);
}

function getStyle(style, snapshot) {
  if (!snapshot.isDropAnimating) {
    return style;
  }
  const { moveTo, curve, duration } = snapshot.dropAnimation;
  // move to the right spot
  const translate = `translate(${moveTo.x}px, ${moveTo.y}px)`;
  // add a bit of turn for fun
  const rotate = 'rotate(0.5turn)';

  // patching the existing style
  return {
    ...style,
    transitionDuration: `0.001s`,
  };
}

function DAG({ classes }) {
  return (
    <DragDropContext onDragEnd={onDragEnd}>
      <Droppable droppableId="dag">
        {(provided) => (
          <div className={classes.root} {...provided.droppableProps} ref={provided.innerRef}>
            <Draggable draggableId="source" index={1}>
              {(pr, snapshot) => (
                <div
                  className={`${classes.node} ${classes.source}`}
                  {...pr.draggableProps}
                  {...pr.dragHandleProps}
                  style={getStyle(pr.draggableProps.style, snapshot)}
                  ref={pr.innerRef}
                >
                  Source
                </div>
              )}
            </Draggable>
            <Draggable draggableId="sink" index={2}>
              {(pr, snapshot) => (
                <div
                  className={`${classes.node} ${classes.source}`}
                  {...pr.draggableProps}
                  {...pr.dragHandleProps}
                  style={getStyle(pr.draggableProps.style, snapshot)}
                  ref={pr.innerRef}
                >
                  Sink
                </div>
              )}
            </Draggable>
            {provided.placeholder}
          </div>
        )}
      </Droppable>
    </DragDropContext>
  );
}

export default withStyles(styles)(DAG);
