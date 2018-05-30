/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import SpotlightModal from 'components/SpotlightSearch/SpotlightModal';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';

require('./Tag.scss');

export default class Tag extends Component {
  static propTypes = {
    value: PropTypes.string,
    scope: PropTypes.string,
    onDelete: PropTypes.func,
    isNativeLink: PropTypes.bool
  };

  static defaultProps = {
    isNativeLink: false
  };

  state = {
    searchModalOpen: false
  }

  toggleSearchModal = () => {
    this.setState({
      searchModalOpen: !this.state.searchModalOpen
    });
  };

  render() {
    let tagClasses = classnames("btn btn-secondary tag-btn", {
      "system-tag": this.props.scope === 'SYSTEM',
      "user-tag": this.props.scope === 'USER'
    });
    return (
      <span className={tagClasses}>
        <span
          onClick = {this.toggleSearchModal}
          className="tag-content"
        >
          <span>{this.props.value}</span>
          {
            this.props.scope === 'USER' ?
              <IconSVG
                name="icon-close"
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
              isNativeLink={this.props.isNativeLink}
            />
          :
            null
        }
      </span>
    );
  }
}
