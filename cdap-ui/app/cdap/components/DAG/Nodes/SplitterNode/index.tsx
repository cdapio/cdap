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
import { genericNodeStyles } from 'components/DAG/Nodes/utilities';
import { IAbstractNodeProps, AbstractNode } from 'components/DAG/Nodes/AbstractNode';
import withStyles from '@material-ui/core/styles/withStyles';
import { MyContext, IDagStore, INodeConfig } from 'components/DAG/DAGProvider';
import { PortPopover } from 'components/DAG/Nodes/SplitterNode/PortPopover';
import If from 'components/If';
import { Map, List } from 'immutable';
import { getPortEndpointId } from 'components/DAG/Nodes/SplitterNode/PortPopover';

export interface IPort {
  name: string;
  label: string;
}

const styles = genericNodeStyles({
  border: `1px solid #4586f3`,
  '&.drag-hover': {
    backgroundColor: 'rgba(69, 134, 243, 0.1)',
  },
});
interface ISplitterNodeProps extends IAbstractNodeProps<typeof styles> {}
class SplitterNodeComponent extends AbstractNode<ISplitterNodeProps> {
  public type = 'splittertransform';

  public getEndpointParams = () => {
    return [];
  };

  public renderEndpoint = (classes) => {
    return (
      <MyContext.Consumer>
        {(context) => {
          const splitter = context.nodes.find((value) => {
            return value.get('id') === this.props.id;
          });
          let ports = splitter.getIn(['config', 'ports']);
          if (!ports) {
            ports = [];
          } else {
            ports = ports.toJS();
          }
          return (
            <div className={`${classes.endpointCircle} ${classes.regularEndpointCircle}`}>
              <div data-node-type={this.type} data-endpoint-type={AbstractNode.ENDPOINT.REGULAR} />
              <If condition={ports.length > 0}>
                <PortPopover
                  nodeName={this.props.name}
                  nodeType={this.type}
                  ports={ports}
                  addEndpoint={this.props.addEndpoint}
                  removeEndpoint={this.props.removeEndpoint}
                />
              </If>
            </div>
          );
        }}
      </MyContext.Consumer>
    );
  };

  private addPortToNode = ({ nodes, updateNode }: IDagStore) => {
    const arr = Array.from(Array(10).keys());
    // const splitter = nodes.find((node) => node.get('id') === this.props.id);
    const ports: IPort[] = arr.map((a) => {
      const id = Date.now();
      const dummyLabel = `port-${id + a}`;
      return { name: dummyLabel, label: dummyLabel };
    });
    updateNode(this.props.id, Map({ ports: List(ports) }));
  };

  private renderPortBtn = () => {
    return (
      <MyContext.Consumer>
        {(context) => (
          <div>
            <button onClick={this.addPortToNode.bind(this, context)}>+</button>
          </div>
        )}
      </MyContext.Consumer>
    );
  };

  public renderContent = () => {
    return (
      <span>
        {this.props.config && this.props.config.label ? this.props.config.label : this.props.id}
        {this.renderPortBtn()}
      </span>
    );
  };
}

const SplitterNode = withStyles(styles)(SplitterNodeComponent);
export { SplitterNode };
