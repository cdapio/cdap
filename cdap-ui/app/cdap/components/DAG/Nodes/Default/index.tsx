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
import 'jsplumb';
import { INodeComponentProps } from 'components/DAG/DAGRenderer';

export class DefaultNode extends React.Component<INodeComponentProps, any> {
  private rootRef: HTMLElement | null;

  public componentDidMount() {
    const source = {
      isSource: true,
      maxConnections: -1, // -1 means unlimited connections
      paintStyle: {
        connectorStyle: {
          lineWidth: 2,
          outlineColor: 'transparent',
          outlineWidth: 4,
          strokeStyle: '#4e5568',
          strokeWidth: 3,
        },
        fill: 'black',
        lineWidth: 3,
        radius: 5,
        stroke: 'black',
      },
    };
    if (this.props.initNode && typeof this.props.initNode === 'function') {
      const initConfig = {
        endPointParams: [
          {
            element: this.rootRef,
            params: source,
            referenceParams: {},
          },
        ],
        makeTargetParams: {
          allowLoopback: false,
          anchor: 'ContinuousLeft',
          dropOptions: { hoverClass: 'drag-hover' },
          isTarget: true,
          uuid: `${this.props.id}-DottedEndPoint`,
        },
        nodeId: this.props.id,
      };
      this.props.initNode(initConfig);
    }
  }

  public render() {
    let style: React.CSSProperties = {
      border: '1px solid',
      display: 'inline-block',
      height: '100px',
      position: 'absolute',
      width: '100px',
    };
    if (this.props.config) {
      style = {
        ...style,
        ...this.props.config.style,
      };
    }
    return (
      <div id={this.props.id} ref={(ref) => (this.rootRef = ref)} style={style}>
        {this.props.config && this.props.config.label ? this.props.config.label : this.props.id}
      </div>
    );
  }
}
