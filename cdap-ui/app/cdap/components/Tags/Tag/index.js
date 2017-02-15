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

import React, {PropTypes, Component} from 'react';
import SpotlightModal from 'components/SpotlightSearch/SpotlightModal';
import classnames from 'classnames';

require('./Tag.scss');

export default class Tag extends Component {
  constructor(props) {
    super(props);
    this.state = {
      searchModalOpen: false,
    };

    this.toggleSearchModal = this.toggleSearchModal.bind(this);
  }

  toggleSearchModal() {
    this.setState({
      searchModalOpen: !this.state.searchModalOpen
    });
  }

  render() {
    let tagClasses = classnames("btn btn-secondary", {
      "system-tags": this.props.scope === 'SYSTEM',
      "user-tags": this.props.scope === 'USER'
    });
    return (
      <span>
        <span
          onClick = {this.toggleSearchModal}
          className={tagClasses}
        >
          <span>{this.props.value}</span>
          {
            this.props.scope === 'USER' ?
              <span
                className="fa fa-times"
                onClick={this.props.onDelete}
              />
            :
              null
          }
        </span>
        {
          this.state.searchModalOpen ?
            <SpotlightModal
              tag={this.props.value}
              target={['app', 'dataset', 'stream']}
              isOpen={this.state.searchModalOpen}
              toggle={this.toggleSearchModal}
            />
          :
            null
        }
      </span>
    );
  }
}

Tag.propTypes = {
  value: PropTypes.string,
  scope: PropTypes.string,
  onDelete: PropTypes.func
};
