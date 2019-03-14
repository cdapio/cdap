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
import { Input } from 'reactstrap';
import { EDIT_PIPELINE } from '../config';
require('./DetailProvider.scss');

class DetailProvider extends React.Component {
  name;
  constructor(props) {
    super(props);
  }

  onNameUpdated(event) {
    this.name =  event.target.value;
    this.props.updateFeatureName(this.name);
  }

  render() {
    return (
      <div className = "detail-step-container">
        <div className='field-row'>
            <div className='name'>Name
              <i className = "fa fa-asterisk mandatory"></i>
            </div>
            <div className='colon'>:</div>
            <Input className='value' type="text" name="name" placeholder='name'  readOnly = {this.props.operationType == EDIT_PIPELINE}
              defaultValue = {this.props.featureName} onChange={this.onNameUpdated.bind(this)}/>
        </div>
        <div className='field-row'>
            <div className='name'>Description</div>
            <div className='colon'>:</div>
            <textarea className='description'></textarea>
        </div>
      </div>
    );
  }
}

export default DetailProvider;
