/*
 * Copyright © 2018 Cask Data, Inc.
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

class DummyComponent extends React.PureComponent {
  public render() {
    return (
      <DAGProvider>
        <MyContext.Consumer>
          {(context) => {
            return (
              <React.Fragment>
                <button onClick={() => context.addNode({
                  id: 'MyNode',
                  name: 'Brand New Node!',
                  properties: {},
                })}>
                  Add new Node
                </button>
              </React.Fragment>
            );
          }}
        </MyContext.Consumer>
      </DAGProvider>
    );
  }
}

export default class DAG extends React.PureComponent {
  public render() {
    return (
      <React.Fragment>
        <DummyComponent />
        <DAGProvider>
          <div>
            <h4> Inside DAG Provider </h4>
            <MyContext.Consumer>
              {(context) => {
                console.log(context);
                return (
                  <React.Fragment>
                    <button onClick={() => context.addNode({
                      id: 'MyNode',
                      name: 'Ma Node!',
                      properties: {},
                    })}>
                      Add Node
                    </button>
                    <div> Node : </div>
                    <pre> {JSON.stringify(context.getNodes())}</pre>
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
