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
import {Dropdown, DropdownMenu} from 'reactstrap';
import classnames from 'classnames';
import AbstractWizard from 'components/AbstractWizard';
import NamespaceStore from 'services/NamespaceStore';
import {Link} from 'react-router';
const shortid = require('shortid');
require('./NamespaceDropdown.less');
export default class NamespaceDropdown extends Component {
  constructor(props) {
    super(props);
    this.state = {
      openDropdown: false,
      openWizard: false,
      namespaceList: NamespaceStore.getState().namespaces,
      currentNamespace: NamespaceStore.getState().selectedNamespace
    };
  }
  componentWillMount() {
    this.setState({
      namespaceList: NamespaceStore.getState().namespaces
    });
    //Load updated data into the namespace dropdown
    NamespaceStore.subscribe(() => {
      this.setState({
        currentNamespace : NamespaceStore.getState().selectedNamespace,
        namespaceList : NamespaceStore.getState().namespaces
      });
    });
  }
  toggle() {
    this.setState({
      openDropdown: !this.state.openDropdown
    });
  }
  showNamespaceWizard() {
    this.setState({
      openWizard: !this.state.openWizard,
      openDropdown: !this.state.openDropdown
    });
  }
  hideNamespaceWizard() {
    this.setState({
      openWizard: false
    });
  }
  selectNamespace(name){
    NamespaceStore.dispatch({
      type: 'SELECT_NAMESPACE',
      payload: {
        selectedNamespace : name
      }
    });
  }
  render() {
    let LinkEl = Link;
    let baseurl = '';
    if (this.props.tag) {
      let basename = document.querySelector('base');
      basename = basename.getAttribute('href') ? basename.getAttribute('href') : null;
      LinkEl = this.props.tag;
      baseurl = `${basename}`;
    }
    return (
      <div>
        <Dropdown
          isOpen={this.state.openDropdown}
          toggle={this.toggle.bind(this)}
          className={classnames({'no-namespace': this.state.namespaceList.length === 1})}
        >
          <div
            className="current-namespace"
            onClick={this.toggle.bind(this)}
          >
            <div className="namespace-text">
              {this.state.currentNamespace}
            </div>
            <span className="fa fa-angle-down pull-right">
            </span>
          </div>
          <DropdownMenu>
            <div className="namespace-list">
              {
                this.state.namespaceList
                  .filter(item => item.name !== this.state.currentNamespace)
                  .map( (item) => {
                    let defaultNamespace = localStorage.getItem('DefaultNamespace');
                    let checkClass = classnames({ "fa fa-star":  defaultNamespace === item.name });
                    let check = <span className={checkClass}></span>;
                    return (
                      <LinkEl
                        href={baseurl + `/ns/${item.name}`}
                        to={baseurl + `/ns/${item.name}`}
                        className="namespace-link"
                        key={shortid.generate()}
                      >
                        <div
                          className="clearfix namespace-container"
                          onClick={this.selectNamespace.bind(this, item.name)}
                        >
                          <span className="namespace-name pull-left">{item.name}</span>
                          <span className="default-ns-section pull-right">
                            {check}
                            {
                              defaultNamespace !== item.name ?
                                (
                                  <span className="default-btn">
                                    <span
                                      className="btn btn-default btn-xs"
                                      onClick={() => localStorage.setItem('DefaultNamespace', item.name)}
                                    >
                                      Default
                                    </span>
                                  </span>
                                )
                                :
                                null
                            }
                          </span>
                        </div>
                      </LinkEl>
                    );
                  })
              }
            </div>
            {
              this.state.namespaceList.length > 1 ?
                (
                  <div
                    className="namespace-action text-center"
                    onClick={this.showNamespaceWizard.bind(this)}
                  >
                    Manage Namespaces
                  </div>
                )
              :
                (
                  <div
                    className="namespace-action text-center"
                    onClick={this.showNamespaceWizard.bind(this)}
                  >
                    Add Namespace
                  </div>
                )
            }
          </DropdownMenu>
        </Dropdown>

        <AbstractWizard
          isOpen={this.state.openWizard}
          onClose={this.hideNamespaceWizard.bind(this)}
          wizardType='add_namespace'
          backdrop={true}
        />
      </div>
    );
  }
}

NamespaceDropdown.propTypes = {
  tag: PropTypes.node
};
