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

import React, {Component, PropTypes} from 'react';

require('./Header.scss');
import HeaderBrand from '../HeaderBrand';
import HeaderNavbarList from '../HeaderNavbarList';
import HeaderActions from '../HeaderActions';
import NamespaceStore from 'services/NamespaceStore';

export default class Header extends Component {
  constructor(props) {
    super(props);
    this.props = props;
    this.state = {
      navbarItemList: this.props.navbarItemList
    };
  }
  sidebarClickNoOp(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    return false;
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      navbarItemList: nextProps.navbarItemList
    });
  }
  render() {

    return (
      <div className="cask-header">
        <div className="navbar navbar-fixed-top">
          <nav className="navbar cdap">
            <HeaderBrand/>
            <HeaderNavbarList
              list={this.state.navbarItemList}
              tag={this.props.tag}
              store={NamespaceStore}
              showOldUI={true}
            />
          <HeaderActions
            tag={this.props.tag}
            product="cdap"
          />
          </nav>
        </div>
      </div>
    );
  }
}

Header.propTypes = {
  navbarItemList: PropTypes.arrayOf(PropTypes.shape({
    linkTo: PropTypes.string,
    title: PropTypes.string
  })),
  tag: PropTypes.string
};
