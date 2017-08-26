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

import React, {Component} from 'react';
import RuleBook from 'components/RulesEngineHome/RuleBook';
import {Input, InputGroup, InputGroupAddon} from 'reactstrap';
import RulesEngineStore, {RULESENGINEACTIONS} from 'components/RulesEngineHome/RulesEngineStore';
import Fuse from 'fuse.js';
import isEmpty from 'lodash/isEmpty';
import isNil from 'lodash/isNil';
import LoadingSVG from 'components/LoadingSVG';
import classnames from 'classnames';
import IconSVG from 'components/IconSVG';
import ImportRulebookWizard from 'components/RulesEngineHome/ImportRulebookWizard';
import T from 'i18n-react';

require('./RulesBooksTab.scss');

const PREFIX = 'features.RulesEngine.RulebooksTab';

export default class RuleBooksTab extends Component {
  state = {
    searchStr: '',
    isOpenImportWizard: false,
    rulebooks: RulesEngineStore.getState().rulebooks.list
  };

  componentDidMount() {
    this.rulesStoreSubscription = RulesEngineStore.subscribe(() => {
      let {rulebooks} = RulesEngineStore.getState();
      if (Array.isArray(rulebooks.list)) {
        this.setState({
          rulebooks: rulebooks.list
        });
      }
    });
  }

  componentWillUnmount() {
    if (this.rulesStoreSubscription) {
      this.rulesStoreSubscription();
    }
  }

  updateSearchStr = (e) => {
    this.setState({
      searchStr: e.target.value
    });
  };

  createNewRuleBook = () => {
    RulesEngineStore.dispatch({
      type: RULESENGINEACTIONS.SETCREATERULEBOOK,
      payload:{
        isCreate: true
      }
    });
  };

  toggleImportWizard = () => {
    this.setState({
      isOpenImportWizard: !this.state.isOpenImportWizard
    });
  };

  renderRulebooks() {

    if (isNil(this.state.rulebooks)) {
      return (<LoadingSVG />);
    }

    if (isEmpty(this.state.searchStr)) {
      return (
        this.state
          .rulebooks
          .map(rulebook => {
            return (<RuleBook bookDetails={rulebook}/>);
          })
      );
    }

    // TODO not sure about performance
    const fuseOptions = {
      caseSensitive: true,
      threshold: 0,
      location: 0,
      distance: 100,
      maxPatternLength: 32,
      keys: [
        "id",
        "user",
        "rules",
        "description",
        "source"
      ]
    };

    let fuse = new Fuse(this.state.rulebooks, fuseOptions);
    return (
      fuse.search(this.state.searchStr)
        .map(rulebook => {
          return (<RuleBook bookDetails={rulebook}/>);
        })
    );
  }

  render() {
    return (
      <div className="rule-books-tab">
        <span className="rule-books-search-label">
          {T.translate(`${PREFIX}.searchLabel`)}
        </span>
        <InputGroup className="rule-books-search-group">
          <InputGroupAddon>
            <IconSVG name="icon-search" />
          </InputGroupAddon>
          <Input
            placeholder={T.translate(`${PREFIX}.searchplaceholder`)}
            value={this.state.searchStr}
            onChange={this.updateSearchStr}
          />
        </InputGroup>
        <div className={classnames("rule-books-container", {
          'loading': isNil(this.state.rulebooks)
        })}>
          <div className="rule-book center">
            <div onClick={this.createNewRuleBook}>
              <strong> {T.translate(`${PREFIX}.createrulebook`)} </strong>
              <IconSVG name="icon-plus" />
            </div>
            <div onClick={this.toggleImportWizard}>
              <strong> {T.translate(`${PREFIX}.importrulebook`)} </strong>
              <IconSVG name="icon-import" />
              <ImportRulebookWizard
                isOpen={this.state.isOpenImportWizard}
                onClose={this.toggleImportWizard}
              />
            </div>
          </div>
          {this.renderRulebooks()}
        </div>
      </div>
    );
  }
}
