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
require('./ServiceStatus.less');
var classNames = require('classnames');
import Datasource from 'services/datasource';
import {Dropdown, DropdownMenu} from 'reactstrap';
import T from 'i18n-react';
import {MyServiceProviderApi} from 'api/serviceproviders';
export default class ServiceStatus extends Component {

  constructor(props){
    super(props);
    this.state = {
      isDropdownOpen : false,
      provisioned : this.props.numProvisioned,
      requested : this.props.numProvisioned,
      editingProvisions : false,
      serviceWarning  : false,
      showError : false,
      provisionsLoading : false,
      enteredProvisionValue : '',
      showBtnError : false,
      errorText: ''
    };
    this.MyDataSrc = new Datasource();
    this.keyDown = this.keyDown.bind(this);
    this.toggleErrorMessage = this.toggleErrorMessage.bind(this);
    this.onProvisionChange = this.onProvisionChange.bind(this);
    this.toggleErrorInBtn = this.toggleErrorInBtn.bind(this);
    this.provisionRowClickHandler = this.provisionRowClickHandler.bind(this);
    this.setProvisionNumber = this.setProvisionNumber.bind(this);
    this.updateProvision = this.updateProvision.bind(this);
    this.toggleDropdown = this.toggleDropdown.bind(this);
  }

  toggleDropdown(){
    this.setState({
      isDropdownOpen : !this.state.isDropdownOpen
    });
  }

  updateProvision(){
    this.setState({
      provisionsLoading : true
    });

    MyServiceProviderApi.setProvisions({
      serviceid : this.props.name
    }, {
      instances : this.state.enteredProvisionValue
    }).subscribe(() => {
      this.setState({
        provisionsLoading : false,
        serviceWarning : false
      });
    }, (err)=> {
        this.setState({
          provisionsLoading : false,
          serviceWarning : true,
          errorText : err.response
        });
    });
  }

  onProvisionChange(e){
    this.setState({
      enteredProvisionValue : e.target.value
    });
  }
  //If the user presses enter, we set the state to reflect the entered value and close the dropdown
  keyDown(e){
    if(e.keyCode !== undefined && e.keyCode === 13){
      //If there is no entered value do not set entered value
      this.setState({
        enteredProvisionValue : e.target.value
      }, () => {
        this.updateProvision();
      });
    }
  }

  setProvisionNumber(){
    this.updateProvision();
  }

  //IF there is an error message, toggle depending on the event type
  toggleErrorMessage(e){
    if(this.state.serviceWarning){
      if(e.type === 'mouseover'){
        this.setState({
          showError : true
        });
      } else if(e.type === 'mouseout'){
        this.setState({
          showError : false
        });
      }
    }
  }
  //IF there is an error message, toggle depending on the event type
  toggleErrorInBtn(e){
    if(this.state.serviceWarning){
      if(e.type === 'mouseover'){
        this.setState({
          showBtnError : true
        });
      } else if(e.type === 'mouseout'){
        this.setState({
          showBtnError : false
        });
      }
    }
  }
  provisionRowClickHandler(e){
    e.stopPropagation();
  }
  render(){

    let statusCircle;
    let circleContent;

    //Corresponds to circle fill colors based on service status
    let green = this.props.status === 'OK' && !this.props.isLoading;
    let red = this.props.status !== 'OK' && !this.props.isLoading;
    let grey = this.props.isLoading;

    let circleClass = classNames({'status-circle-green' : green, 'status-circle-red' : red, "status-circle-grey" : grey, 'circle-dropdown-active' : this.state.isDropdownOpen});

    if(this.state.provisionsLoading){
      circleContent = <span className="fa fa-spinner fa-spin fa-fw" />;
    }

    if(this.props.status === 'OK'){
      circleContent = this.state.provisioned;
    }

    statusCircle = (<div className={circleClass}>
                    {circleContent}
                   </div>);

    let logUrl = this.MyDataSrc.constructUrl({
      _cdapPath : `/system/services/${this.props.name}/logs`
    });

    logUrl = `/downloadLogs?type=raw&backendUrl=${encodeURIComponent(logUrl)}`;

    let provisionBtnClasses = classNames('btn btn-default btn-primary set-provision-btn', {'provision-btn-with-warning' : this.state.serviceWarning});

    return (
      <div
        onClick={this.toggleDropdown}
        className="service-status"
        onMouseOver={this.toggleErrorMessage}
        onMouseOut={this.toggleErrorMessage}
      >
        {statusCircle}
        <div className="status-label">
          {T.translate(`features.Management.Services.${this.props.name.split('.').join('_')}`)}
          {
            this.state.serviceWarning ?
              <span
                className="fa fa-exclamation-triangle"
                aria-hidden="true"
              >
                {
                  this.state.showError && !this.state.isDropdownOpen?
                  <div className="service-error-message">
                    {this.state.errorText}
                  </div>
                  :
                  null
                }
              </span>
              :
              null
          }
        </div>
        <div className="service-dropdown-container pull-right">
          <Dropdown
            isOpen={this.state.isDropdownOpen}
            toggle={this.toggleDropdown}
            className="service-dropdown"
          >
            <span className="fa fa-caret-down service-dropdown-caret">
            </span>
            <DropdownMenu>
              <div className="dropdown-service-name service-dropdown-item">
                <div className="status-label">
                  {T.translate(`features.Management.Services.${this.props.name.split('.').join('_')}`)}
                </div>
              </div>
              <a href={logUrl} target="_blank">
                <div className="service-dropdown-item">
                  <span className="fa fa-file-text" />
                    View Logs
                </div>
              </a>
              <div
                className="provision-dropdown-item service-dropdown-item"
                onClick={this.provisionRowClickHandler}
                onMouseOver={this.toggleErrorMessage}
                onMouseOut={this.toggleErrorMessage}
              >
                {T.translate('features.Management.Services.requested')}
                <input
                  className="provision-input"
                  placeholder={this.state.provisioned}
                  onChange={this.onProvisionChange}
                  onKeyDown={this.keyDown}
                />
                <button
                  onMouseOver={this.toggleErrorInBtn}
                  onMouseOut={this.toggleErrorInBtn}
                  className={provisionBtnClasses}
                  onClick={this.setProvisionNumber}
                >
                  {T.translate('features.Management.Services.setBtn')}
                  {
                    this.state.serviceWarning ?
                      <span
                        className="warning-triangle fa fa-exclamation-triangle"
                        aria-hidden="true"
                      >
                        {
                          this.state.showBtnError && this.state.isDropdownOpen ?
                          <div className="service-error-message-inline">
                            <div className="fa fa-caret-up" />
                            {this.state.errorText}
                          </div>
                          :
                          null
                        }
                      </span>
                      :
                      null
                  }
                </button>
              </div>
            </DropdownMenu>
          </Dropdown>
        </div>
      </div>
    );
  }
}

ServiceStatus.propTypes = {
  name: PropTypes.string,
  status: PropTypes.string,
  isLoading: PropTypes.bool,
  numProvisioned: PropTypes.number
};
