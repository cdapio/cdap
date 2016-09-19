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
import ReactDOM from 'react-dom';
import ResourceCenterEntity from '../ResourceCenterEntity';
import StreamCreateWizard from 'components/CaskWizards/StreamCreate';

require('./ResourceCenter.less');

export default class ResourceCenter extends Component {
  constructor(props) {
    super(props);
    this.state = {
      createStreamWizard: false
    };
  }
  toggleWizard(wizardName) {
    this.setState({
      [wizardName]: !this.state[wizardName]
    });
  }
  closeWizard(wizardContainer) {
    ReactDOM.unmountComponentAtNode(wizardContainer);
  }
  render(){
    return (
      <div>
        <div className="cask-resource-center">
          {
            Array
              .apply(null, {length: 8})
              .map((e, index) => (
                <ResourceCenterEntity
                  title="Stream"
                  description="Lorem Ipsum has been the industry's standard dummy text ever since the 1500s"
                  actionLabel="Create"
                  key={index}
                  onClick={this.toggleWizard.bind(this, 'createStreamWizard')}
                />
              ))
          }
        </div>
        {
          this.state.createStreamWizard ?
            <StreamCreateWizard
              isOpen={this.state.createStreamWizard}
              onClose={this.toggleWizard.bind(this, 'createStreamWizard')}
            />
          :
            null
        }
      </div>
    );
  }
}
