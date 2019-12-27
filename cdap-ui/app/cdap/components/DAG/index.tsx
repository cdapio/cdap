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
import { DAGProvider, MyContext } from 'components/DAG/DAGProvider';
import { DefaultNode } from 'components/DAG/Nodes/Default';
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

export default class DAG extends React.PureComponent {
  public render() {
    return (
      <React.Fragment>
        <DAGProvider>
          <div>
            <h4> Inside DAG Provider </h4>
            <MyContext.Consumer>
              {(context) => {
                return (
                  <React.Fragment>
                    <button
                      onClick={() =>
                        context.addNode(
                          fromJS({
                            config: {
                              label: `Node_${Date.now()
                                .toString()
                                .substring(5)}`,
                            },
                            id: `Node_${Date.now()
                              .toString()
                              .substring(5)}`,
                            name: 'Ma Node!',
                          })
                        )
                      }
                    >
                      Add Node
                    </button>
                    <DAGRenderer
                      nodes={context.nodes}
                      connections={context.connections}
                      onConnection={context.addConnection}
                      onConnectionDetached={context.removeConnection}
                      onDeleteNode={context.removeNode}
                      jsPlumbSettings={defaultJsPlumbSettings}
                      // registerTypes={registerTypes}
                    >
                      {context.nodes.map((node, i) => {
                        const nodeObj = node.toJS();
                        return <DefaultNode {...nodeObj} key={i} />;
                      })}
                    </DAGRenderer>
                  </React.Fragment>
                );
              }}
            </MyContext.Consumer>
          </div>
        </DAGProvider>
      </React.Fragment>
    );
  }
}
