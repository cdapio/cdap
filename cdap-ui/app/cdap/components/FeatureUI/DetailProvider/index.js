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
import React from 'react';
import ValidatedInput from 'components/ValidatedInput';
import types from 'services/inputValidationTemplates';
import { EDIT_PIPELINE } from '../config';
import PropTypes from 'prop-types';

require('./DetailProvider.scss');

class DetailProvider extends React.Component {
  name;
  constructor(props) {
    super(props);

    this.state = {
      inputs: {
        'name': {
          'error': '',
          'required': true,
          'template': 'NAME',
          'label': 'name'
        },
      },
    };
  }

  onNameUpdated(key, event) {
    const isValid = types[this.state.inputs[key]['template']].validate(event.target.value);
    this.setState({
      inputs: {
        ...this.state.inputs,
        [key]: {
          ...this.state.inputs[key],
          'error': isValid ? '' : 'Invalid Input',
        }
      }
    });
    // Validation Function in Store check if value is empty
    // So by setting name to empty string (if there is error
    // in input) we are invalidating submission in Store.
    this.name = isValid ? event.target.value : '';
    this.props.updateFeatureName(this.name);
  }

  render() {
    return (
      <div className = "detail-step-container">
        <div className='field-row'>
            <div className='name'>Name
              { this.state.inputs['name']['required'] &&
                <i className = "fa fa-asterisk mandatory"></i>
              }
            </div>
            <div className='colon'>:</div>
            <ValidatedInput
              className='value'
              label={this.state.inputs['name']['label']}
              inputInfo={types[this.state.inputs['name']['template']].getInfo()}
              validationError={this.state.inputs['name']['error']}
              type="text"
              placeholder='name'
              readOnly = {this.props.operationType == EDIT_PIPELINE}
              defaultValue = {this.props.featureName}
              onChange={this.onNameUpdated.bind(this, 'name')}
            />
        </div>
        {/* <div className='field-row'>
            <div className='name'>Description</div>
            <div className='colon'>:</div>
            <textarea className='description'></textarea>
        </div> */}
      </div>
    );
  }
}

export default DetailProvider;
DetailProvider.propTypes = {
  updateFeatureName: PropTypes.func,
  operationType: PropTypes.string,
  featureName: PropTypes.string
};
