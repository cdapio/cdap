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

import PropTypes from 'prop-types';
import React, {PureComponent} from 'react';
import {Link} from 'react-router-dom';
import IconSVG from 'components/IconSVG';

require('./EntityTopPanel.scss');

export default class EntityTopPanel extends PureComponent {
  static propTypes = {
    breadCrumbAnchorLink: PropTypes.string,
    breadCrumbAnchorLabel: PropTypes.string,
    title: PropTypes.string,
    closeBtnAnchorLink: PropTypes.oneOfType([PropTypes.string, PropTypes.func])
  };

  renderBreadCrumnAnchorLink = () => {
    if (!this.props.breadCrumbAnchorLink) {
      return null;
    }
    return (
      <div>
        <Link
          to={this.props.breadCrumbAnchorLink}
        >
          <span className="arrow-left">
            &laquo;
          </span>
          <span className="breadcrumb-label">
            {this.props.breadCrumbAnchorLabel}
          </span>
        </Link>
        <span className="divider"> | </span>
      </div>
    );
  };

  renderTitle = () => {
    return (
      <h5 className="overview-heading">{this.props.title}</h5>
    );
  };

  renderCloseBtn = () => {
    if (!this.props.closeBtnAnchorLink) {
      return null;
    }

    if (typeof this.props.closeBtnAnchorLink === 'function') {
      return (
        <h5 className="toppanel-close-btn">
          <IconSVG
            name="icon-close"
            onClick={this.props.closeBtnAnchorLink}
          />
        </h5>
      );
    }

    return (
      <h5 className="toppanel-close-btn">
        <Link
          to={this.props.closeBtnAnchorLink}
        >
          <IconSVG name="icon-close" />
        </Link>
      </h5>
    );
  };

  render() {
    return (
      <div className="entity-top-panel">
        <div className="toppanel-title-container">
          {this.renderBreadCrumnAnchorLink()}
          {this.renderTitle()}
        </div>
        {this.renderCloseBtn()}
      </div>
    );
  }
}
