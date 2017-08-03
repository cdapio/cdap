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
import RulesEngineStore from 'components/RulesEngineHome/RulesEngineStore';
import Rx from 'rx';
import {isDescendant} from 'services/helpers';
import T from 'i18n-react';

require('./RulebooksPopover.scss');
const PREFIX = 'features.RulesEngine.RulebooksPopover';

export default class RulebooksPopover extends Component {
  static propTypes = {
    onChange: PropTypes.func
  };

  state = {
    showPopover: false
  };

  componentDidMount() {
    this.documentClick$ = Rx.Observable.fromEvent(document, 'click')
    .subscribe((e) => {
      if (!this.popover) {
        return;
      }

      if (isDescendant(this.popover, e.target) || !this.state.showPopover) {
        return;
      }

      this.togglePopover();
    });
  }

  togglePopover = () => {
    this.setState({
      showPopover: !this.state.showPopover
    });
  }

  onRulebookSelect = (rbid) => {
    if (this.props.onChange) {
      this.props.onChange(rbid);
    }
    this.togglePopover();
  };

  renderPopover = () => {
    if (!this.state.showPopover) {
      return null;
    }
    let {rulebooks} = RulesEngineStore.getState();
    return (
      <div
        className="sub-menu"
      >
        {
          rulebooks.list.map(rb => {
            return (
              <div onClick={this.onRulebookSelect.bind(this, rb.id)}>
                {rb.id}
              </div>
            );
          })
        }
      </div>
    );
  };

  render () {
    return (
      <div
        className="rule-book-popover"
        onClick={this.togglePopover}
        ref={(ref) => this.popover = ref}
      >
        <div className="btn btn-secondary"> {T.translate(`${PREFIX}.addToRulebookbtn`)} </div>
        {this.renderPopover()}
      </div>
    );
  }
}
