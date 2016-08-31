/*
 * Copyright © 2016 Cask Data, Inc.
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

import React, {Component} from 'react';
import Datasource from '../../services/datasource';

export default class ConnectionExample extends Component {
  constructor(props) {
    super(props);

    this.datasrc = new Datasource();

    this.state = {
      namespaces: []
    };
    let reqObj = {
      _cdapPath: '/namespaces'
    };

    let namespaceStream = this.datasrc.poll(reqObj);

    this.stream = namespaceStream.subscribe((data) => {
      this.setState({namespaces: data});
    });
  }

  stopPoll() {
    this.stream.dispose();
  }

  render() {
    return (
      <div>
        <h1> Connection </h1>

        <h3> Namespaces</h3>

        <ul>
          {this.state.namespaces.map((ns, index) => <li key={index}>{ns.name}</li>)}
        </ul>

        <button onClick={this.stopPoll.bind(this)}>stop</button>
      </div>
    );
  }
}
