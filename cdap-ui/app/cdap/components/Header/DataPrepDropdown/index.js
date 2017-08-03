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
import {NavLink} from 'react-router-dom';
require('./DataPrepDropdown.scss');

const NavLinkWrapper = ({children, url, nativeLink, isActive, ...attributes}) => {
  if (nativeLink) {
    return (
      <a
        href={`/cdap/${url}`}
        {...attributes}
        className={classnames({
          'active': isActive()
        })}
      >
        {children}
      </a>
    );
  }
  return (
    <NavLink
      to={`/${url}`}
      isActive={isActive}
      {...attributes}
    >
      {children}
    </NavLink>
  );
};
NavLinkWrapper.propTypes = {
  children: PropTypes.node,
  url: PropTypes.string,
  nativeLink: PropTypes.bool,
  isActive: PropTypes.func
};

export default class DataPrepDown extends Component {
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
    let baseurl = `ns/${namespace}`;
    let dataprepurl = `${baseurl}/dataprep`;
    let rulesengineurl = `${baseurl}/rulesengine`;

    return (
      <Dropdown
        className={classnames("daraprep-dropdown", {
          'active': location.pathname.match(/\/dataprep$/) || location.pathname.match(/\/rulesengine$/)
        })}
        isOpen={this.state.toggleDropdown}
        toggle={this.toggleMetadataDropdown}
      >
        <DropdownToggle
          caret
          className={classnames({'active': location.pathname.indexOf('dataprep') !== -1})}
        >
          Data Preparation
        </DropdownToggle>
        <CustomDropdownMenu>
          <DropdownItem tag="li">
            <NavLinkWrapper
              nativeLink={this.props.nativeLink}
              url={dataprepurl}
              isActive={this.isDataPrepActive}
            >
              Home
            </NavLinkWrapper>
          </DropdownItem>
          <DropdownItem tag="li">
            <NavLinkWrapper
              nativeLink={this.props.nativeLink}
              url={rulesengineurl}
              isActive={this.isRulesEnginedActive}
            >
              Rules Management
            </NavLinkWrapper>
          </DropdownItem>
        </CustomDropdownMenu>
      </Dropdown>
    );
  }
}

DataPrepDown.propTypes = {
  nativeLink: PropTypes.bool
};
