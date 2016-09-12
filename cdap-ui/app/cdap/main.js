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

// require('./services/i18n');
// import Test from './services/i18n';
import React, {Component, PropTypes} from 'react';
import ReactDOM from 'react-dom';

require('../ui-utils/url-generator');
require('font-awesome-webpack!./styles/font-awesome.config.js');
require('./styles/lib-styles.less');
require('./styles/common.less');
require('./styles/main.less');

import Management from './components/Management';
import Dashboard from './components/Dashboard';
import Home from './components/Home';
import CdapHeader from './components/CdapHeader';
import Footer from './components/Footer';
import SplashScreen from './components/SplashScreen';
import ConnectionExample from './components/ConnectionExample';
import Experimental from './components/Experimental';
import cookie from 'react-cookie';
import {MyNamespaceApi} from './api/namespace';
import Redirect from 'react-router/Redirect';
import Router from 'react-router/BrowserRouter';
import Match from 'react-router/Match';
import Miss from 'react-router/Miss';
import Store from './services/store/store';

class CDAP extends Component {
  constructor(props) {
    super(props);
    this.props = props;
    this.version = '4.0.0';
    this.namespace = Store.getState().selectedNamespace;
    this.namespaceList = Store.getState().namespaceList;
    this.pathname = props.pathname;
    this.render = this.render.bind(this);
    Store.subscribe(this.render);
    this.state = {
      selectedNamespace : Store.getState().selectedNamespace
    };
  }

  componentWillMount(){
    var selectedNamespace;
    let defaultNsSet = false;

    if(!this.state.selectedNamespace){
      this.setState({
        selectedNamespace : cookie.load('CDAP_Auth_User')
      });

      Store.dispatch({
          type: 'SELECT_NAMESPACE',
          payload: {
            selectedNamespace : this.state.selectedNamespace
          }
      });
    }

    //Polls for namespace data
    MyNamespaceApi.pollList()
      .subscribe((res) => {

        if(res.length > 0){
          for(var i = 0; i < res; i++){
            if(res[i].description === 'Default Namespace'){
              selectedNamespace = res[i].name;
            }
          }

          if(!selectedNamespace){
            selectedNamespace = res[0].name;
          }

          Store.dispatch({
            type: 'UPDATE_NAMESPACES',
            payload: {
              namespaces : res
            }
          });

          if(!defaultNsSet){
            Store.dispatch({
              type: 'SELECT_NAMESPACE',
              payload: {
                selectedNamespace : selectedNamespace
              }
            });

            this.setState({
              selectedNamespace : selectedNamespace
            });

            defaultNsSet = true;
          }

        } else {
          //To-Do: No namespaces returned ; throw error / redirect
        }
      });
  }

  findNamespace(name){
    var namespaces = Store.getState().namespaces;

    if(!namespaces){
      return;
    }

    for(var i = 0; i < namespaces.length; i++){
      if(namespaces[i].name === name){
        return false;
      }
    }
    return false;
  }


  render() {
    if( window.CDAP_CONFIG.securityEnabled &&
        !cookie.load('CDAP_Auth_Token')
     ){
      //authentication failed ; redirect to another page
      window.location.href = window.getAbsUIUrl({
        uiApp: 'login',
        redirectUrl: location.href,
        clientId: 'cdap'
      });

      return null;
    }

    this.namespace = Store.getState().selectedNamespace;
    this.namespaceList = Store.getState().namespaces;

    if(!this.namespace || !this.namespaceList){
      return null;
    }

    return (
      <Router basename="/cask-cdap">
        <div className="cdap-container">
          <CdapHeader pathname={location.pathname} />
          <SplashScreen />
          <div className="container-fluid">
            {this.props.children}
          </div>
          <Footer version={this.version} />
          <Match exactly pattern="/" render={() => (<Redirect to={`/ns/${this.state.selectedNamespace}`} />)} />
          <Match exactly pattern="/notfound" component={Missed} />
          <Match exactly pattern="/management" component={Management} />
          <Match exactly pattern="/ns/:namespace" component={Home} />
          <Match exactly pattern="/ns/:namespace/dashboard" component={Dashboard} />
          <Match pattern="/Experimental" component={Experimental} />
          <Match pattern="/socket-example" component={ConnectionExample} />
          <Miss component={Missed} />
        </div>
      </Router>
    );
  }
}

CDAP.propTypes = {
  children: React.PropTypes.node,
  pathname: PropTypes.string,
  params: PropTypes.object
};

function Missed({ location }) {
  return (
    <div>
      <h2>404 - Page Not Found</h2>
      <p>Page {location.pathname} not found</p>
    </div>
  );
}

const propTypes = {
  location : PropTypes.object
};

Missed.propTypes = propTypes;

ReactDOM.render(
  <CDAP />,
  document.getElementById('app-container')
);
