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
import { ButtonDropdown, DropdownToggle, DropdownMenu, DropdownItem } from 'reactstrap';
import {MyAppApi} from 'api/app';
import NamespaceStore from 'services/NamespaceStore';

export default class VersionsDropdown extends Component {
  constructor(props) {
    super(props);

    this.toggle = this.toggle.bind(this);
    this.state = {
      dropdownOpen: false,
      versions: []
    };
  }
  fetchVersions() {
    let namespace = NamespaceStore.getState().selectedNamespace;
    let appId = this.props.entity.id;
    MyAppApi
      .getVersions({
        namespace,
        appId
      })
      .subscribe((res) => {
        let versions = res.map(version => {
          if (version === '-SNAPSHOT') {
            return '1.0.0-SNAPSHOT';
          }
          return version;
        });
        this.setState({
          versions
        });
      });
  }
  componentWillMount() {
    this.fetchVersions();
  }
  componentWillReceiveProps(newProps) {
    let {entity} = newProps;
    if (this.props.entity.id !== entity.id) {
      this.setState({
        entity
      });
    }
  }
  toggle() {
    this.setState({
      dropdownOpen: !this.state.dropdownOpen
    });
  }

  render() {
    return (
      <ButtonDropdown isOpen={this.state.dropdownOpen} toggle={this.toggle}>
        <DropdownToggle caret>
          {this.state.versions.length ? this.state.versions[0]: 'N/A'}
        </DropdownToggle>
        <DropdownMenu>
          {
            this.state.versions.map( version => {
              return (
                <DropdownItem>{version}</DropdownItem>
              );
            })
          }
        </DropdownMenu>
      </ButtonDropdown>
    );
  }
}
VersionsDropdown.propTypes = {
  entity: PropTypes.object
};
