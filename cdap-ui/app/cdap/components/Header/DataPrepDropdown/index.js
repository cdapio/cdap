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

import React, {Component, PropTypes} from 'react';
import { DropdownToggle, DropdownItem, Dropdown } from 'reactstrap';
import CustomDropdownMenu from 'components/CustomDropdownMenu';
import classnames from 'classnames';
import NamespaceStore from 'services/NamespaceStore';
import NavLinkWrapper from 'components/NavLinkWrapper';
import T from 'i18n-react';

require('./DataPrepDropdown.scss');

export default class DataPrepDropdown extends Component {

  static propTypes = {
    nativeLink: PropTypes.bool
  };

  state = {
    toggleDropdown: false
  };

  toggleMetadataDropdown = () => {
    this.setState({
      toggleDropdown: !this.state.toggleDropdown
    });
  }

  isRulesEnginedActive = (match, location = window.location) => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let rulesenginepath = `/ns/${namespace}/ruleengine`;
    if (match && match.isExact) {
      return true;
    }
    return location.pathname.startsWith(rulesenginepath);
  };

  isDataPrepActive = (match, location = window.location) => {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let dataprepBasePath = `/ns/${namespace}/dataprep`;
    let connectionsBasePath = `/ns/${namespace}/connections`;
    if (!match) {
      if (location.pathname.startsWith(dataprepBasePath) || location.pathname.startsWith(connectionsBasePath)) {
        return true;
      }
      return false;
    }
    if (match.isExact) { return true; }

    if (location.pathname.startsWith(dataprepBasePath) || location.pathname.startsWith(connectionsBasePath)) {
      return true;
    }
    return false;
  };

  render() {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    let baseurl = `/ns/${namespace}`;
    let dataprepurl = `${baseurl}/dataprep`;
    let rulesengineurl = `${baseurl}/rulesengine`;

    return (
      <Dropdown
        className="daraprep-dropdown"
        isOpen={this.state.toggleDropdown}
        toggle={this.toggleMetadataDropdown}
      >
        <DropdownToggle
          caret
          className={classnames({
            'active': location.pathname.match(/\/dataprep$/) || location.pathname.match(/\/rulesengine$/)
          })}
        >
          {T.translate(`features.Navbar.dataprepLabel`)}
        </DropdownToggle>
        <CustomDropdownMenu>
          <DropdownItem tag="li">
            <NavLinkWrapper
              isNativeLink={this.props.nativeLink}
              to={dataprepurl}
              className={classnames({
                'active': this.isDataPrepActive()
              })}
            >
              {T.translate(`features.Navbar.Dataprep.home`)}
            </NavLinkWrapper>
          </DropdownItem>
          <DropdownItem tag="li">
            <NavLinkWrapper
              isNativeLink={this.props.nativeLink}
              to={rulesengineurl}
              className={classnames({
                'active': this.isRulesEnginedActive()
              })}
            >
              {T.translate(`features.Navbar.Dataprep.rulesmgmt`)}
            </NavLinkWrapper>
          </DropdownItem>
        </CustomDropdownMenu>
      </Dropdown>
    );
  }
}

