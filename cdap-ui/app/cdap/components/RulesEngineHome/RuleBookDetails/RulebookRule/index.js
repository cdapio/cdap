/*
 * Copyright © 2017 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import {Col} from 'reactstrap';
import { DragSource, DropTarget } from 'react-dnd';
import flow from 'lodash/flow';
import classnames from 'classnames';
import { Sparklines, SparklinesLine, SparklinesBars } from 'react-sparklines';
import IconSVG from 'components/IconSVG';
import isNil from 'lodash/isNil';
import T from 'i18n-react';

const PREFIX = 'features.RulesEngine.RulebookRule';
const SPARKLINECOLOR = '#0099ff';
const SVGHEIGHT = 20;
const SVGWIDTH = 80;

const ItemTypes = {
  Rule: 'RULEBOOKRULE'
};

const dragSource = {
  beginDrag(props) {
    return {
      id: props.id,
      index: props.index,
    };
  },
};

const dropTarget = {
  hover(props, monitor, component) {
    const dragIndex = monitor.getItem().index;
    const hoverIndex = props.index;

    // Don't replace items with themselves
    if (dragIndex === hoverIndex) {
      return;
    }

    let {componentRef} = component.decoratedComponentInstance;
    if (!componentRef) {
      return;
    }

    // Determine rectangle on screen
    const hoverBoundingRect = componentRef.getBoundingClientRect();

    // Get vertical middle
    const hoverMiddleY = (hoverBoundingRect.bottom - hoverBoundingRect.top) / 2;

    // Determine mouse position
    const clientOffset = monitor.getClientOffset();

    // Get pixels to the top
    const hoverClientY = clientOffset.y - hoverBoundingRect.top;

    // Only perform the move when the mouse has crossed half of the items height
    // When dragging downwards, only move when the cursor is below 50%
    // When dragging upwards, only move when the cursor is above 50%

    // Dragging downwards
    if (dragIndex < hoverIndex && hoverClientY < hoverMiddleY) {
      return;
    }

    // Dragging upwards
    if (dragIndex > hoverIndex && hoverClientY > hoverMiddleY) {
      return;
    }

    // Time to actually perform the action
    props.onRuleSort(dragIndex, hoverIndex);

    monitor.getItem().index = hoverIndex;
  },
  drop: (props, monitor) => {
    let dragIndex = monitor.getItem().index;
    let hoverIndex = props.index;
    props.onRuleSort(dragIndex, hoverIndex);
  }
};


class RulebookRule extends Component {
  static propTypes = {
    index: PropTypes.number,
    rule: PropTypes.object,
    onRemove: PropTypes.func,
    onRuleSort: PropTypes.func,
    connectDragSource: PropTypes.func.isRequired,
    connectDropTarget: PropTypes.func.isRequired,
    isDragging: PropTypes.bool.isRequired
  };

  componentRef = null;

  render() {
    let {index, rule, onRemove, connectDropTarget, connectDragSource, isDragging} = this.props;
    return connectDragSource(connectDropTarget(
      <div
        className={classnames("row", {
          'dragging': isDragging
        })}
        ref={ref => {
          if (ref) {
            this.componentRef = ref;
          }
        }}
      >
        <Col xs={1}>{index + 1}</Col>
        <Col xs={2}>{rule.id}</Col>
        <Col xs={5}>{rule.description}</Col>
        <Col xs={1}>
          <button
            className="btn btn-link remove-button"
            href
            onClick={() => onRemove(rule.id)}
          >
            {T.translate(`${PREFIX}.remove`)}
          </button>
        </Col>
        <Col xs={1}>
          <IconSVG
            className="move-icon"
            name="icon-arrows-v"
          />
        </Col>
        <Col xs={2}>
          {
            isNil(rule.metric) ? null :
              <Sparklines data={rule.metric} svgWidth={SVGWIDTH} svgHeight={SVGHEIGHT} limit={10}>
                <SparklinesBars style={{ fill: SPARKLINECOLOR, fillOpacity: ".15" }} />
                <SparklinesLine style={{ stroke: SPARKLINECOLOR, fill: "none" }} />
              </Sparklines>
          }
        </Col>
      </div>
    ));
  }
}

export default flow([
  DragSource(ItemTypes.Rule, dragSource, (connect, monitor) => ({
    connectDragSource: connect.dragSource(),
    isDragging: monitor.isDragging(),
  })),
  DropTarget(ItemTypes.Rule, dropTarget, connect => ({
    connectDropTarget: connect.dropTarget(),
  }))
])(RulebookRule);
