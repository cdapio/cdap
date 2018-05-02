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
import IconSVG from 'components/IconSVG';
require('./AccordionTitle.scss');

export default class AccordionTitle extends PureComponent {
  static propTypes = {
    id: PropTypes.string.isRequired,
    onTabPaneClick: PropTypes.func,
    activePane: PropTypes.string,
    size: PropTypes.oneOf(["small", "medium", "large"]),
    children: PropTypes.node
  };

  static defaultProps = {
    size: "medium"
  };

  onClickHandler = () => {
    if (this.props.onTabPaneClick) {
      this.props.onTabPaneClick(this.props.id);
    }
  };

  render() {
    let {size} = this.props;
    return (
      <div
        className={`accordion-title ${size}`}
        onClick={this.onClickHandler}
      >
        <div>
          {
            this.props.id === this.props.activePane ?
              <IconSVG name="icon-caret-down" />
            :
              <IconSVG name="icon-caret-right" />
          }
        </div>
        <div>
            {this.props.children}
        </div>
      </div>
    );
  }
}
