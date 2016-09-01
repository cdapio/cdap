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

import Card from './components/Card';
import CardActionFeedback from './components/CardActionFeedback';

import * as util from './utils';
import Footer from './components/Footer';

require('./styles/lib-styles.less');
require('./login.less');

class Login extends Component {
  constructor(props) {
    super(props);
    this.state = {
      username: localStorage.getItem('login_username') || '',
      password: '',
      message: '',
      formState: false,
      rememberUser: false
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
        cookie.save('CDAP_Auth_Token', res.access_token);
        cookie.save('CDAP_Auth_User', this.state.username);
        var queryObj = util.getQueryParams(location.search);
        queryObj.redirectUrl = queryObj.redirectUrl || '/';
        window.location.href = queryObj.redirectUrl;
      });
  }
  onUsernameUpdate(e) {
    this.setState({
      username: e.target.value,
      formState: e.target.value.length && this.state.password.length,
      message: '',
    });
  }
  onPasswordUpdate(e) {
    this.setState({
      password: e.target.value,
      formState: this.state.username.length && e.target.value.length,
      message: '',
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
              <label htmlFor="username">
                Username
              </label>
              <input
                className="form-control"
                name="username"
                value={this.state.username}
                onChange={this.onUsernameUpdate.bind(this)}
              />
            </div>
            <div className="form-group">
              <label>
                Password
              </label>
              <input
                className="form-control"
                onChange={this.onPasswordUpdate.bind(this)}
                type="password"
              />
            </div>
            <div className="form-group">
              <div className="clearfix">
                <div className="pull-left">
                  <div className="checkbox">
                    <label>
                      <input
                        type="checkbox"
                        value={this.state.rememberUser}
                        onClick={this.rememberUser.bind(this)}
                      />
                      Remember me
                    </label>
                  </div>
                </div>
                <button
                  type="submit"
                  className="btn btn-primary pull-right"
                  disabled={!this.state.formState}
                  onClick={this.login.bind(this)}
                >
                  Login
                </button>
              </div>
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
