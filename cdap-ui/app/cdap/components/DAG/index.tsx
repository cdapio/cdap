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

import * as React from 'react';
import Button from '@material-ui/core/Button';
import ThemeWrapper from 'components/ThemeWrapper';
import { DAGProvider, MyContext } from 'components/DAG/DAGProvider';
import { SourceNode } from 'components/DAG/Nodes/SourceNode';
import { TransformNode } from 'components/DAG/Nodes/TransformNode';
import { SinkNode } from 'components/DAG/Nodes/SinkNode';
import { AlertPublisherNode } from 'components/DAG/Nodes/AlertPublisherNode';
import { ErrorNode } from 'components/DAG/Nodes/ErrorNode';
import { ConditionNode } from 'components/DAG/Nodes/ConditionNode';
import { SplitterNode } from 'components/DAG/Nodes/SplitterNode';
import { DAGRenderer } from 'components/DAG/DAGRenderer';
import {
  defaultJsPlumbSettings,
  defaultConnectionStyle,
  selectedConnectionStyle,
  dashedConnectionStyle,
  solidConnectionStyle,
  conditionTrueConnectionStyle,
  conditionFalseConnectionStyle,
} from 'components/DAG/JSPlumbSettings';
import { fromJS } from 'immutable';
import withStyles, { WithStyles, StyleRules } from '@material-ui/core/styles/withStyles';
import ReactPanZoom from '@ajainarayanan/react-pan-zoom';
import IconSVG from 'components/IconSVG';

const registerTypes = {
  connections: {
    basic: defaultConnectionStyle,
    conditionFalse: conditionFalseConnectionStyle,
    conditionTrue: conditionTrueConnectionStyle,
    dashed: dashedConnectionStyle,
    selected: selectedConnectionStyle,
    solid: solidConnectionStyle,
  },
  endpoints: {},
};

const styles = (): StyleRules => {
  return {
    root: {
      margin: '20px',
    },
    diagramContainer: {
      position: 'relative',
    },
    btnStyles: {
      color: 'white',
      margin: '0 5px',
    },
    sourceBtn: {
      backgroundColor: '#48c038',
    },
    transformBtn: {
      backgroundColor: '#4586f3',
    },
    sinkBtn: {
      backgroundColor: '#8367df',
    },
    alertBtn: {
      backgroundColor: '#ffba01',
    },
    errorBtn: {
      backgroundColor: '#d40001',
    },
    conditionBtn: {
      backgroundColor: '#4e5568',
    },
    panContainer: {
      height: 'calc(100vh - 192px)',
      width: 'calc(100% - 20px)',
      overflow: 'hidden',
      display: 'flex',
      justifyContent: 'center',
      alignItems: 'center',
      zIndex: 1,
      margin: '10px',
      border: '1px solid grey',
      '&> *': {
        height: '100%',
        width: '100%',
      },
    },
    containerActions: {
      width: '50px',
      backgroundColor: '#eee',
      zIndex: 1000,
      position: 'absolute',
      right: '13px',
      top: '44px',
      '&> *': {
        borderRadius: 0,
        borderBottom: '1px solid white',
      },
      '&> :first-child': {
        borderTopRightRadius: '4px',
        borderTopLeftRadius: '4px',
      },
      '&> :last-child': {
        borderBottomLeftRadius: '4px',
        borderBottomRightRadius: '4px',
      },
    },
    containerActionBtn: {
      minWidth: 'unset',
      width: '50px',
      background: '#bbb',
      height: '30px',
    },
  };
};
interface IDAGProps extends WithStyles<typeof styles> {}
class DAG extends React.PureComponent<IDAGProps> {
  public state = {
    dx: 0,
    dy: 0,
    zoom: 1,
  };

  public addNode = (addNode, type, showAlertAndError) => {
    addNode(
      fromJS({
        config: {
          label: `Node_${Date.now()
            .toString()
            .substring(5)}`,
          showAlert: showAlertAndError,
          showError: showAlertAndError,
        },
        type,
        id: `Node_${Date.now()
          .toString()
          .substring(5)}`,
        name: 'Node_bleh',
      })
    );
  };
  public nodeTypeToComponentMap = {
    source: SourceNode,
    transform: TransformNode,
    sink: SinkNode,
    alertpublisher: AlertPublisherNode,
    error: ErrorNode,
    condition: ConditionNode,
    splittertransform: SplitterNode,
  };
  public getButton = (type, label, className, addNode, showAlertAndError = false) => (
    <Button
      className={className}
      variant="contained"
      color="primary"
      onClick={this.addNode.bind(this, addNode, type, showAlertAndError)}
    >
      {label}
    </Button>
  );

  public zoomIn = () => {
    this.setState({
      zoom: this.state.zoom + 0.1,
    });
  };
  public zoomOut = () => {
    this.setState({
      zoom: this.state.zoom - 0.1,
    });
  };
  public resetPan = () => {
    this.setState({
      zoom: 1,
      dx: 0,
      dy: 0,
    });
  };
  public onPan = (dx, dy) => {
    this.setState({ dx, dy });
  };

  private renderNodeBtns = (classes, context) => {
    return (
      <div>
        {this.getButton(
          'source',
          'Add Source',
          `${classes.btnStyles} ${classes.sourceBtn}`,
          context.addNode
        )}
        {this.getButton(
          'transform',
          'Add Transform',
          `${classes.btnStyles} ${classes.transformBtn}`,
          context.addNode
        )}
        {this.getButton(
          'transform',
          'Add Transform (w/alert & error)',
          `${classes.btnStyles} ${classes.transformBtn}`,
          context.addNode,
          true
        )}
        {this.getButton(
          'sink',
          'Add Sink',
          `${classes.btnStyles} ${classes.sinkBtn}`,
          context.addNode
        )}
        {this.getButton(
          'alertpublisher',
          'Add Alert',
          `${classes.btnStyles} ${classes.alertBtn}`,
          context.addNode
        )}
        {this.getButton(
          'error',
          'Add Error',
          `${classes.btnStyles} ${classes.errorBtn}`,
          context.addNode
        )}
        {this.getButton(
          'condition',
          'Add Condition',
          `${classes.btnStyles} ${classes.conditionBtn}`,
          context.addNode
        )}
        {this.getButton(
          'splittertransform',
          'Add Splitter',
          `${classes.btnStyles} ${classes.transformBtn}`,
          context.addNode
        )}
      </div>
    );
  };
  private renderContainerActions = (classes) => {
    return (
      <div className={classes.containerActions}>
        <Button className={classes.containerActionBtn} onClick={this.zoomIn}>
          <IconSVG name="icon-zoomIn" />
        </Button>
        <Button className={classes.containerActionBtn} onClick={this.zoomOut}>
          <IconSVG name="icon-zoomout" />
        </Button>
        <Button className={classes.containerActionBtn} onClick={this.resetPan}>
          <IconSVG name="icon-fit" />
        </Button>
      </div>
    );
  };
  public render() {
    const { classes } = this.props;
    return (
      <div className="diagram-container">
        <ThemeWrapper>
          <DAGProvider>
            <div className={classes.root}>
              <h4> DAG Prototype </h4>
              <MyContext.Consumer>
                {(context) => {
                  return (
                    <div className={classes.diagramContainer}>
                      {this.renderNodeBtns(classes, context)}
                      {this.renderContainerActions(classes)}
                      <div className={classes.panContainer}>
                        <ReactPanZoom
                          zoom={this.state.zoom}
                          pandx={this.state.dx}
                          pandy={this.state.dy}
                          onPan={this.onPan}
                        >
                          <DAGRenderer
                            nodes={context.nodes}
                            connections={context.connections}
                            addConnection={context.addConnection}
                            removeConnection={context.removeConnection}
                            removeNode={context.removeNode}
                            jsPlumbSettings={defaultJsPlumbSettings}
                          >
                            {context.nodes.map((node, i) => {
                              const nodeObj = node.toJS();
                              const Component = this.nodeTypeToComponentMap[nodeObj.type];
                              return <Component {...nodeObj} key={i} />;
                            })}
                          </DAGRenderer>
                        </ReactPanZoom>
                      </div>
                    </div>
                  );
                }}
              </MyContext.Consumer>
            </div>
          </DAGProvider>
        </ThemeWrapper>
      </div>
    );
  }
}

const StyledDAG = withStyles(styles)(DAG);
export default StyledDAG;
