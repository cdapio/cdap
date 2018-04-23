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

import PropTypes from 'prop-types';

import React, {PureComponent} from 'react';
import {NavLink} from 'react-router-dom';

export default class NavLinkWrapper extends PureComponent {
  static propTypes = {
    children: PropTypes.node,
    to: PropTypes.string,
    isNativeLink: PropTypes.bool
  };
  render() {
    if (this.props.isNativeLink) {
      return (
        <a
          {...this.props}
          href={this.props.to}
        >
          {this.props.children}
        </a>
      );
    }
    return (
      <NavLink
        {...this.props}
        to={this.props.to}
      >
        {this.props.children}
      </NavLink>
    );
  }
}
