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
import ReactDOM from 'react-dom';
import 'whatwg-fetch';
import cookie from 'react-cookie';

import Card from 'components/Card';
import CardActionFeedback from 'components/CardActionFeedback';

import * as util from './utils';
import Footer from '../cdap/components/Footer';
import ValidatedInput from '../cdap/components/ValidatedInput';
import types from '../cdap/services/inputValidationTemplates';

require('./styles/lib-styles.scss');
require('./login.scss');
import T from 'i18n-react';
T.setTexts(require('./text/text-en.yaml'));


class Login extends Component {
  constructor(props) {
    super(props);
    this.state = {
      username: localStorage.getItem('login_username') || '',
      password: '',
      message: '',
      formState: false,
      rememberUser: false,
      inputs: this.getValidationState(),
    };
  }

  getValidationState = () => {
    return {
      name: {
        error: '',
        required: true,
        template: 'NAME',
        label: 'userName',
      },
      password: {
        error: '',
        required: false,
        template: 'NAME',
        label: 'password',
      },
    };
  }

  login(e) {
    e.preventDefault();
    if (this.state.rememberUser) {
      localStorage.setItem('login_username', this.state.username);
    }
    fetch('/login', {
      method: 'POST',
      headers: {'Accept': 'application/json', 'Content-Type': 'application/json'},
      body: JSON.stringify({
        username: this.state.username,
        password: this.state.password
      })
    })
      .then((response) => {
        if (response.status >= 200 && response.status < 300) {
          return response.json();
        } else {
          this.setState({
            message: 'Login failed. Username or Password incorrect.'
          });
          return Promise.reject();
        }
      })
      .then((res) => {
        cookie.save('CDAP_Auth_Token', res.access_token, { path: '/'});
        cookie.save('CDAP_Auth_User', this.state.username);
        var queryObj = util.getQueryParams(location.search);
        queryObj.redirectUrl = queryObj.redirectUrl || '/';
        window.location.href = queryObj.redirectUrl;
      });
  }

  onUsernameUpdate(e) {

    let inputsValue = {...this.state.inputs};
    const isValid = types[this.state.inputs.name.template].validate(e.target.value);
    let errorMsg = '';
    if (e.target.value && !isValid) {
      errorMsg = 'Invalid input, can not contain any xml tag';// types[this.state.inputs.name.template].getErrorMsg();
    }
    inputsValue.name.error = errorMsg;

    this.setState({
      username: e.target.value,
      formState: e.target.value.length && this.state.password.length,
      message: '',
      inputs: inputsValue,
    });
  }

  onPasswordUpdate(e) {
    let inputsValue = {...this.state.inputs};
    const isValid = types[this.state.inputs.password.template].validate(e.target.value);
    let errorMsg = '';
    if (e.target.value && !isValid) {
      errorMsg = 'Invalid input, can not contain any xml tag';//types[this.state.inputs.password.template].getErrorMsg();
    }
    inputsValue.password.error = errorMsg;

    this.setState({
      password: e.target.value,
      formState: this.state.username.length && e.target.value.length,
      message: '',
      inputs: inputsValue,
    });
  }




  rememberUser() {
    this.setState({
      rememberUser: true
    });
  }
  render() {
    let footer;
    if (this.state.message) {
      footer = (
        <CardActionFeedback
          type="DANGER"
          message={this.state.message}
        />
      );
    }

    return (
      <div>
        <Card footer={footer}>
          <div className="cdap-logo"></div>
          <form
            role="form"
            onSubmit={this.login.bind(this)}
          >
            <div className="form-group">
              <ValidatedInput
                  type="text"
                  label={this.state.inputs.name.label}
                  placeholder={T.translate('login.placeholders.username')}
                  inputInfo={types[this.state.inputs.name.template].getInfo()}
                  validationError={this.state.inputs.name.error}
                  value={this.state.username}
                  onChange={this.onUsernameUpdate.bind(this)}
                />
            </div>
            <div className="form-group">
              <ValidatedInput
                    type="password"
                    label={this.state.inputs.password.label}
                    placeholder={T.translate('login.placeholders.password')}
                    inputInfo={types[this.state.inputs.password.template].getInfo()}
                    validationError={this.state.inputs.password.error}
                    onChange={this.onPasswordUpdate.bind(this)}
                  />
            </div>
            <div className="form-group">
              <div className="clearfix">
                <div className="float-xs-left">
                  <div className="checkbox form-check">
                    <label className="form-check-label">
                      <input
                        type="checkbox"
                        className="form-check-input"
                        value={this.state.rememberUser}
                        onClick={this.rememberUser.bind(this)}
                      />
                    {T.translate('login.labels.rememberme')}
                    </label>
                  </div>
                </div>
              </div>
            </div>
            <div className="form-group">
              <button
                id="submit"
                type="submit"
                className="btn btn-primary btn-block"
                disabled={!this.state.formState || this.state.inputs.name.error.length > 0 || this.state.inputs.password.error.length > 0}
                onClick={this.login.bind(this)}
              >
                {T.translate('login.labels.loginbtn')}
              </button>
            </div>
          </form>
        </Card>
      </div>
    );
  }
}
ReactDOM.render(
  <Login />,
  document.getElementById('login-form')
);
ReactDOM.render(
  <Footer />,
  document.getElementById('footer-container')
);
