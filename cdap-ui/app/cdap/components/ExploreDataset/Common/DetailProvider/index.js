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
import { Input } from 'reactstrap';
import PropTypes from 'prop-types';
import { EDIT } from '../constant';
import isNil from 'lodash/isNil';
import cloneDeep from 'lodash/cloneDeep';

require('./DetailProvider.scss');

class DetailProvider extends React.Component {
  name;
  outputPath;
  extraConfigurations;
  constructor(props) {
    super(props);
    this.state = {
      hadoopSave: false
    };
  }

  componentWillMount() {
    this.extraConfigurations = cloneDeep(this.props.extraConfigurations);
    this.outputPath = isNil(this.extraConfigurations.outputPath) ? "" : this.extraConfigurations.outputPath;
    const hadoopSave = (!isNil(this.extraConfigurations.hadoopSave) && this.extraConfigurations.hadoopSave == "Yes") ? true : false;
    this.setState({
      hadoopSave: hadoopSave
    });
    this.extraConfigurations["hadoopSave"] = hadoopSave ? "Yes" : "No";
    this.extraConfigurations["outputPath"] = this.outputPath;
    this.props.setExtraConfigurations(this.extraConfigurations);
  }

  onNameUpdated(event) {
    this.name = event.target.value;
    this.props.updatePipelineName(this.name);
  }

  onOutputPathUpdated(event) {
    this.outputPath = event.target.value;
    this.extraConfigurations["hadoopSave"] = "Yes";
    this.extraConfigurations["outputPath"] = this.outputPath;
    this.props.setExtraConfigurations(this.extraConfigurations);
  }

  onSaveToHadoopChange(evt) {
    if (evt.target.checked) {
      this.extraConfigurations["hadoopSave"] = "Yes";
      this.extraConfigurations["outputPath"] = this.outputPath;
      this.props.setExtraConfigurations(this.extraConfigurations);
    } else {
      this.extraConfigurations["hadoopSave"] = "No";
      this.extraConfigurations["outputPath"] = null;
      this.props.setExtraConfigurations(this.extraConfigurations);
    }
    this.setState({
      hadoopSave: evt.target.checked
    });
  }

  render() {
    return (
      <div className="detail-step-container">
        <div className='field-row'>
          <div className='name'>Name
              <i className="fa fa-asterisk mandatory"></i>
          </div>
          <div className='colon'>:</div>
          <Input className='value' type="text" name="name" placeholder='name' readOnly={this.props.operationType == EDIT}
            defaultValue={this.props.pipelineName} onChange={this.onNameUpdated.bind(this)} />
        </div>
        <div className="config-header-label">
          <Input
            type="checkbox"
            onChange={this.onSaveToHadoopChange.bind(this)}
            checked={this.state.hadoopSave}
          />
          <span>Save to Hadoop</span>
        </div> {
          this.state.hadoopSave &&
          <div className='field-row'>
            <div className='name'>Output Path
                <i className="fa fa-asterisk mandatory"></i>
            </div>
            <div className='colon'>:</div>
            <Input className='value' type="text" name="name" placeholder='name'
              defaultValue={this.outputPath} onChange={this.onOutputPathUpdated.bind(this)} />
          </div>
        }
      </div>
    );
  }
}

export default DetailProvider;
DetailProvider.propTypes = {
  extraConfigurations: PropTypes.any,
  setExtraConfigurations: PropTypes.func,
  updatePipelineName: PropTypes.func,
  operationType: PropTypes.string,
  pipelineName: PropTypes.string
};
