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

import React, {Component} from 'react';
import { Dropdown, DropdownMenu, DropdownItem } from 'reactstrap';
import cookie from 'react-cookie';
import {Link} from 'react-router';
import PlusButton from '../PlusButton';
import T from 'i18n-react';
import Store from 'services/store/store.js';
import SpotlightSearch from 'components/SpotlightSearch';
require('./HeaderActions.less');
var classNames = require('classnames');
const shortid = require('shortid');

export default class HeaderActions extends Component {

  constructor(props) {
    super(props);
    this.state = {
      settingsOpen : false,
      name : Store.getState().userName,
      namespaceList : [],
      namespaceOpen : false,
      currentNamespace: Store.getState().selectedNamespace
    };
    this.logout = this.logout.bind(this);
    this.toggleSettingsDropdown = this.toggleSettingsDropdown.bind(this);
    this.namespaceList = [];
    this.namespaceMap = '';
    this.toggleNamespaceDropdown = this.toggleNamespaceDropdown.bind(this);
    this.loadNamespacesFromStore = this.loadNamespacesFromStore.bind(this);
  }

  componentWillMount(){

    //Load the namespaces into the dropdown from the store
    this.namespaceList = Store.getState().namespaces;
    if(this.namespaceList){
      this.loadNamespacesFromStore(this.namespaceList);
    }

    //Load updated data into the namespace dropdown
    Store.subscribe(() => {
      this.namespaceList = Store.getState().namespaces;
      let namespaceToUpdate = Store.getState().selectedNamespace;
      let username = Store.getState().userName;

      if(this.namespaceList){
        this.loadNamespacesFromStore(this.namespaceList, namespaceToUpdate);
      }

      this.setState({
        currentNamespace : namespaceToUpdate,
        name : username
      });
    });
  }

  toggleSettingsDropdown(){
    this.setState({
      settingsOpen : !this.state.settingsOpen
    });
  }

  logout() {
    cookie.remove('CDAP_Auth_Token', { path: '/' });
    cookie.remove('CDAP_Auth_User', { path: '/' });
    window.location.href = window.getAbsUIUrl({
      uiApp: 'login',
      redirectUrl: location.href,
      clientId: 'cdap'
    });
  }

  toggleNamespaceDropdown(){
    this.setState({
      namespaceOpen : !this.state.namespaceOpen
    });
  }

  loadNamespacesFromStore (namespaceList, currentNamespace) {
    if(typeof namespaceList === 'undefined'){
      return;
    }

    this.namespaceMap = namespaceList
      .filter(item => item.name !== currentNamespace)
      .map( (item) => {
        let defaultNamespace = localStorage.getItem('DefaultNamespace');
        let checkClass = classNames({ "fa fa-star":  defaultNamespace === item.name });
        let check = <span className={checkClass}></span>;
        return (
          <Link to={`/ns/${item.name}`} key={shortid.generate()}>
            <div
              className="clearfix namespace-container"
              onClick={this.selectNamespace.bind(this, item.name)}>
              <span className="namespace-name pull-left">{item.name}</span>
              <span className="default-ns-section pull-right">
                {check}
                <span className="default-btn">
                  <span
                    className="btn btn-default btn-xs"
                    onClick={() => localStorage.setItem('DefaultNamespace', item.name)}>
                    Default
                  </span>
                </span>
              </span>
            </div>
          </Link>
        );
      });
  }

  selectNamespace(name){
    localStorage.setItem('NS', name);

    this.setState({
      namespaceOpen: false
    });

    Store.dispatch({
      type: 'SELECT_NAMESPACE',
      payload: {
        selectedNamespace : name
      }
    });
  }

  render() {

    let topRow = '';
    let signoutRow = '';

    if(this.state.name && window.CDAP_CONFIG.securityEnabled){
      topRow = (
        <div>
          <div className="dropdown-item dropdown-name-row">
            <span>{T.translate('features.Navbar.HeaderActions.signedInAs')}</span>
            <span className="dropdown-name">
              {this.state.name}
            </span>
          </div>
          <DropdownItem divider />
        </div>
      );

      signoutRow = (
        <div>
          <DropdownItem divider />
          <div
            className="dropdown-item"
            onClick={this.logout}
          >
            <span className="dropdown-icon fa fa-sign-out"></span>
            {T.translate('features.Navbar.HeaderActions.logout')}
          </div>
        </div>
      );
    }

    return (
      <div className="header-actions">
        <ul className="navbar-list pull-right">
          <div className="navbar-item">
            <SpotlightSearch />
          </div>
          <div className="navbar-item">
            <span className="fa fa-bell"></span>
          </div>
          <PlusButton className="navbar-item" />
          <div
            className="navbar-item settings-dropdown navbar-cog"
            onClick={this.toggleSettingsDropdown}
          >
            <span
              className={classNames('fa', 'fa-cog', {'menu-open' : this.state.settingsOpen})}
            >
            </span>
            <span
              className={classNames('navbar-cog-arrow', {'hidden' : !this.state.settingsOpen})}
            >
            </span>
            <Dropdown
              isOpen={this.state.settingsOpen}
              toggle={this.toggleSettingsDropdown}
              className="header-actions-dropdown"
            >
              <DropdownMenu>
                {topRow}
                <div className="dropdown-item">
                  <a href="http://cask.co/community">
                    <span className="dropdown-icon fa fa-life-ring"></span>
                    {T.translate('features.Navbar.HeaderActions.support')}
                  </a>
                </div>
                <div className="dropdown-item">
                  <a href="http://cask.co/">
                    <span className="dropdown-icon fa fa-home"></span>
                    {T.translate('features.Navbar.HeaderActions.caskHome')}
                  </a>
                </div>
                <div className="dropdown-item">
                  <a href="http://docs.cask.co/">
                    <span className="dropdown-icon fa fa-file"></span>
                    {T.translate('features.Navbar.HeaderActions.documentation')}
                  </a>
                </div>
                {signoutRow}
              </DropdownMenu>
            </Dropdown>
          </div>
          <div className="navbar-item namespace-dropdown">
            <Dropdown
              isOpen={this.state.namespaceOpen}
              toggle={this.toggleNamespaceDropdown}
              className={classNames({'no-namespace': this.namespaceMap.length === 0})}
            >
              <div
                className="current-namespace"
                onClick={this.toggleNamespaceDropdown}
              >
                <div className="namespace-text">
                  {this.state.currentNamespace}
                </div>
                <span className="fa fa-angle-down pull-right">
                </span>
              </div>
              <DropdownMenu>
                <div className="namespace-list">
                  {this.namespaceMap}
                </div>
                {
                  this.namespaceMap.length > 0 ?
                    (<div className="namespace-action text-center">
                      Manage Namespaces
                    </div>)
                  :
                    (
                      <div className="namespace-action text-center">
                        Add Namespace
                      </div>
                    )


                }
              </DropdownMenu>
            </Dropdown>
          </div>
        </ul>
      </div>
    );
  }
}
