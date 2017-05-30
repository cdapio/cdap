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

/*
  This is a direct copy paste from reactstrap's source. There is only one reason to do this.
  We can't upgrade reactstrap to 4.* yet as it has upgraded to bootstrap that defaults to flexbox
  now and we can't make that change at this point in the release. So this artifact stays here until we have
  upgraded reactstrap.
*/
import React, { Component, PropTypes } from 'react';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
require('./ExpandableMenu.scss');

export default class ExpandableMenu extends Component {
  constructor(props) {
    super(props);
    this.state = {
      showMenuItems: true
    };
    this.toggle = this.toggle.bind(this);
  }
  toggle() {
    this.setState({
      showMenuItems: !this.state.showMenuItems
    });
  }
  render() {
    if (this.props.children.length !== 2) {
      return null;
    }
    return (
      <div className={classnames("expandable-menu menu-item expandable", this.props.className)}>
        <div
          onClick={this.toggle}
          className="expandable-title"
        >
          <span className="fa fa-fw">
            <IconSVG
              name={this.state.showMenuItems ? 'icon-caret-down' : 'icon-caret-right'}
            />
          </span>
          {this.props.children[0]}
        </div>
        {
          this.state.showMenuItems ?
            this.props.children[1]
          :
            null
        }
      </div>
    );
  }
}
ExpandableMenu.propTypes = {
  // Assumption: First child is the menu title and the Second child is the actual menu items.
  children: PropTypes.number,
  className: PropTypes.string
};
