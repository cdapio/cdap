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
import withStyles from '@material-ui/core/styles/withStyles';
import { IRegisterTypesProps } from 'components/DAG/DAGRenderer';
import { IAbstractNodeProps, AbstractNode } from 'components/DAG/Nodes/AbstractNode';
import { genericNodeStyles } from 'components/DAG/Nodes/utilities';
import ButtonBase from '@material-ui/core/ButtonBase';
import { blue, grey, bluegrey } from 'components/ThemeWrapper/colors';
require('./conditionNode.scss');

const DIMENSION_OF_CONDITION_NODE = 105;

const styles = genericNodeStyles({
  border: `1px solid #4e5568`,
  width: `${DIMENSION_OF_CONDITION_NODE}px`,
  height: `${DIMENSION_OF_CONDITION_NODE}px`,
  '&.drag-hover': {
    backgroundColor: 'rgba(78, 85, 104, 0.1)',
  },
});

interface IConditionNodeProps extends IAbstractNodeProps<typeof styles> {}

class ConditionNodeComponent extends AbstractNode<IConditionNodeProps> {
  public type = 'condition';
  public yesEndpointRef: HTMLElement | null;
  public noEndpointRef: HTMLElement | null;

  public static commonConnectionStyle = {
    strokeWidth: 2,
    lineWidth: 2,
    outlineColor: 'transparent',
    outlineWidth: 4,
    dashstyle: '2 4',
  };

  public static dashedConnectionStyle = {
    stroke: bluegrey[200],
    ...ConditionNodeComponent.commonConnectionStyle,
  };

  public static conditionYesConnectionStyle = {
    stroke: blue[200],
    ...ConditionNodeComponent.commonConnectionStyle,
  };

  public static conditionNoConnectionStyle = {
    stroke: grey[200],
    ...ConditionNodeComponent.commonConnectionStyle,
  };

  public static getYesEndpointConfig = () => {
    return {
      anchor: 'Right',
      isSource: true,
      connectorStyle: ConditionNodeComponent.conditionYesConnectionStyle,
      overlays: [
        [
          'Label',
          { label: 'Yes', id: 'yesLabel', location: [0, -1.0], cssClass: 'condition-label' },
        ],
      ],
    };
  };
  public static getNoEndpointConfig = () => {
    return {
      anchor: [0.5, 1, 0, 1, 2, 0], // same as Bottom but moved right 2px,
      isSource: true,
      connectorStyle: ConditionNodeComponent.conditionNoConnectionStyle,
      overlays: [
        [
          'Label',
          {
            label: 'No',
            id: 'noLabel',
            location: [0.5, -1.5],
            cssClass: 'condition-label',
          },
        ],
      ],
    };
  };
  public getEndpointParams = () => {
    const yesEndpointConfigs = ConditionNodeComponent.getYesEndpointConfig();
    const noEndpointConfigs = ConditionNodeComponent.getNoEndpointConfig();
    return [
      {
        element: this.yesEndpointRef,
        params: yesEndpointConfigs,
        referenceParams: {},
        labelId: 'yesLabel',
      },
      {
        element: this.noEndpointRef,
        params: noEndpointConfigs,
        referenceParams: {},
        labelId: 'noLabel',
      },
    ];
  };

  public getRegisterTypes = (): IRegisterTypesProps => {
    return {
      connections: {
        dashed: { paintStyle: ConditionNodeComponent.dashedConnectionStyle },
        conditionYes: ConditionNodeComponent.conditionYesConnectionStyle,
        conditionNo: ConditionNodeComponent.conditionNoConnectionStyle,
      },
      endpoints: {},
    };
  };

  public checkForValidIncomingConnection = (connObj) => {
    const sourceNodeType = connObj.connection.source.getAttribute('data-node-type');
    const targetNodeType = connObj.connection.target.getAttribute('data-node-type');
    if (sourceNodeType === 'condition') {
      const sourceEndpointDOMElement = document.getElementById(connObj.sourceId);
      if (sourceEndpointDOMElement) {
        if (
          sourceEndpointDOMElement.getAttribute('data-endpoint-type') ===
          AbstractNode.ENDPOINT.CONDITION_YES
        ) {
          connObj.connection.setType('conditionYes');
        }
        if (
          sourceEndpointDOMElement.getAttribute('data-endpoint-type') ===
          AbstractNode.ENDPOINT.CONDITION_NO
        ) {
          connObj.connection.setType('conditionNo');
        }
      }
    } else if (targetNodeType === 'condition') {
      connObj.connection.setType('dashed');
    }
    return true;
  };

  public render() {
    const { classes } = this.props;
    const YesEndpoint = () => (
      <div
        className={`${classes.endpointCircle} ${classes.regularEndpointCircle}`}
        ref={(ref) => (this.yesEndpointRef = ref)}
        data-node-type={this.type}
        data-endpoint-type={AbstractNode.ENDPOINT.CONDITION_YES}
      >
        <div className={classes.endpointCaret} />
      </div>
    );
    const NoEndpoint = () => (
      <div
        className={`${classes.endpointCircle} ${classes.bottomEndpointCircle} ${classes.conditionNoEndpointCircle}`}
        ref={(ref) => (this.noEndpointRef = ref)}
        data-node-type={this.type}
        data-endpoint-type={AbstractNode.ENDPOINT.CONDITION_NO}
      >
        <div className={classes.bottomEndpointCaret} />
      </div>
    );
    return (
      <ButtonBase id={this.props.id} className={classes.root} data-node-type={this.type}>
        <span>
          {this.props.config && this.props.config.label ? this.props.config.label : this.props.id}
        </span>
        <YesEndpoint />
        <NoEndpoint />
      </ButtonBase>
    );
  }
}

const ConditionNode = withStyles(styles)(ConditionNodeComponent);
export { ConditionNode, DIMENSION_OF_CONDITION_NODE };
