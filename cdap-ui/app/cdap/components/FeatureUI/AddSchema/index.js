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

/* eslint react/prop-types: 0 */
import React from 'react';

require('./AddSchema.scss');

class AddSchema extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      title: props.title ? props.title : 'Add Schema',
      type: props.type ? props.type : 'NEW',
    };
  }

  onOperation(type) {
    if (this.props.operation) {
      this.props.operation(type, this.props.data);
    }
  }

  render() {
    return (
      <div className= { this.state.type == 'NEW' ? "add-container" : "scheme-container"}>
        <div className="tilte">{this.state.title}</div>
        {
          (this.state.type == 'NEW') ? <i className="fa fa-plus-circle add-operation" onClick={this.onOperation.bind(this,'ADD')}></i>:
          <div>
            <i className="fa fa-trash delete-operation" onClick={this.onOperation.bind(this,'REMOVE')}></i>
          </div>

        }
      </div>
    );
  }
}

export default AddSchema;
