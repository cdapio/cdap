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
import {getRuleBooks, resetStore, getRules, setActiveRulebook} from 'components/RulesEngineHome/RulesEngineStore/RulesEngineActions';
import RulesEngineStore, {RULESENGINEACTIONS} from 'components/RulesEngineHome/RulesEngineStore';
import RulesEngineAlert from 'components/RulesEngineHome/RulesEngineAlert';
import NamespaceStore from 'services/NamespaceStore';
import MyRulesEngineApi from 'api/rulesengine';
import LoadingSVGCentered from 'components/LoadingSVGCentered';
import RulesEngineServiceControl from 'components/RulesEngineHome/RulesEngineServiceControl';
import Helmet from 'react-helmet';
import T from 'i18n-react';
import RulesEngineWrapper from 'components/RulesEngineHome/RulesEngineWrapper';
import isNil from 'lodash/isNil';

const PREFIX = 'features.RulesEngine.Home';

export default class RulesEngineHome extends Component {

  static propTypes = {
    embedded: PropTypes.bool,
    onApply: PropTypes.func,
    rulebookid: PropTypes.string
  };

  defaultProps = {
    onApply: () => {}
  };

  constructor(props) {
    super(props);
    if (this.props.embedded) {
      RulesEngineStore.dispatch({
        type: RULESENGINEACTIONS.SETINTEGRATIONEMBEDDED
      });
    }
  }

  componentWillReceiveProps(nextProps) {
    if (nextProps.embedded) {
      RulesEngineStore.dispatch({
        type: RULESENGINEACTIONS.SETINTEGRATIONEMBEDDED
      });
    }
  }

  state = {
    loading: true,
    backendDown: false,
    embedded: this.props.embedded || false
  };

  componentDidMount() {
    if (this.props.embedded) {
      // This is to avoid the jankiness when loading rules engine in modal
      // modal has a dropin animation from top and react's render during this animation
      // creates jankness while rendering. This is to smooth it out.
      setTimeout(() => {
        this.checkIfBackendUp();
      }, 1000);
    } else {
      this.checkIfBackendUp();
    }
  }

  componentDidUnmount() {
    resetStore();
  }

  fetchRulesAndRulebooks = () => {
    getRuleBooks();
    getRules();
    if (!isNil(this.props.rulebookid)) {
      setActiveRulebook(this.props.rulebookid);
    }
  };

  checkIfBackendUp() {
    let {selectedNamespace: namespace} = NamespaceStore.getState();
    MyRulesEngineApi
      .ping({namespace})
      .subscribe(
        () => {
          this.setState({
            loading: false
          });
          this.fetchRulesAndRulebooks();
        },
        () => {
          this.setState({
            backendDown: true,
            loading: false
          });
        }
      );
  }

  onServiceStart = () => {
    this.setState({
      loading: false,
      backendDown: false
    });
    this.fetchRulesAndRulebooks();
  };

  render() {
    if (this.state.loading) {
      return (
        <LoadingSVGCentered />
      );
    }

    if (this.state.backendDown) {
      return (
        <RulesEngineServiceControl
          onServiceStart={this.onServiceStart}
        />
      );
    }

    return (
      <div className="rules-engine-home">
        <Helmet
          title={T.translate(`${PREFIX}.pageTitle`)}
        />
          <RulesEngineWrapper onApply={this.props.onApply}/>
        <RulesEngineAlert />
      </div>
    );
  }
}
