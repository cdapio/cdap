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
import classnames from 'classnames';
const PREFIX = 'features.DataPrep.Directives.ExtractFields';
import T from 'i18n-react';
import UsingPatternsModal from 'components/DataPrep/Directives/ExtractFields/UsingPatternsModal';
import UsingDelimiterModal from 'components/DataPrep/Directives/ExtractFields/UsingDelimiterModal';
import CutMenuItem from 'components/DataPrep/Directives/ExtractFields/UsingPositions/CutMenuItem';
import {execute} from 'components/DataPrep/store/DataPrepActionCreator';
import DataPrepStore from 'components/DataPrep/store';
import DataPrepActions from 'components/DataPrep/store/DataPrepActions';
import {setPopoverOffset} from 'components/DataPrep/helper';
import debounce from 'lodash/debounce';

require('./ExtractFields.scss');
export default class ExtractFields extends Component {
  constructor(props) {
    super(props);
    this.state = {
      activeModal: null
    };
    this.parseUsingPatterns = this.parseUsingPatterns.bind(this);
    this.parseUsingDelimiters = this.parseUsingDelimiters.bind(this);
    this.preventPropagation = this.preventPropagation.bind(this);
    this.handleUsingDelimiters = this.handleUsingDelimiters.bind(this);
  }

  componentDidMount() {
    this.calculateOffset = setPopoverOffset.bind(this, document.getElementById('extract-fields-directive'));
    this.offsetCalcDebounce = debounce(this.calculateOffset, 1000);
  }

  componentDidUpdate() {
    if (this.props.isOpen && this.calculateOffset) {
      this.calculateOffset();
    }
  }

  componentWillUnmount() {
    window.removeEventListener('resize', this.offsetCalcDebounce);
  }

  renderDetail() {
    if (!this.props.isOpen) { return null; }

    return (
      <div
        className="extract-fields second-level-popover"
        onClick={this.preventPropagation}
      >
        <div className="extract-field-options">
          <div
            onClick={this.parseUsingPatterns}
            className="option"
          >
            {T.translate(`${PREFIX}.patternSubmenuTitle`)}
          </div>
        </div>
        <div className="extract-field-options">
          <div
            onClick={this.parseUsingDelimiters}
            className="option"
          >
            {T.translate(`${PREFIX}.delimitersSubmenuTitle`)}
          </div>
        </div>
        <div className="extract-field-options">
          <div
            onClick={this.parseUsingPosition}>
            <CutMenuItem
              column={Array.isArray(this.props.column) ? this.props.column[0] : this.props.column}
              onComplete={this.props.onComplete}
            />
          </div>
        </div>
      </div>
    );
  }

  parseUsingPatterns() {
    this.setState({
      activeModal: (
        <UsingPatternsModal
          isOpen={true}
          column={this.props.column}
          onComplete={this.props.onComplete}
          onClose={() => this.setState({activeModal: null})}
        />
      )
    });
  }

  parseUsingDelimiters() {
    this.setState({
      activeModal: (
        <UsingDelimiterModal
          onApply={this.handleUsingDelimiters}
          onClose={() => this.setState({activeModal: null})}
        />
      )
    });
  }
  handleUsingDelimiters(delimiter) {
    let column = this.props.column;
    let directive = `split-to-columns ${column} ${delimiter}`;
    this.execute([directive]);
  }
  execute(addDirective) {
    execute(addDirective)
      .subscribe(() => {
        this.props.onComplete();
        this.setState({activeModal: null});
      }, (err) => {
        console.log('error', err);

        DataPrepStore.dispatch({
          type: DataPrepActions.setError,
          payload: {
            message: err.message || err.response.message
          }
        });
      });
  }

  renderModal() {
    return this.state.activeModal;
  }

  preventPropagation(e) {
    e.stopPropagation();
    e.nativeEvent.stopImmediatePropagation();
    e.preventDefault();
  }

  render() {
    return (
      <div
        id="extract-fields-directive"
        className={classnames('clearfix action-item', {
          'active': this.props.isOpen
        })}
      >
        <span className="option">
          {T.translate(`${PREFIX}.title`)}
        </span>

        <span className="float-xs-right">
          <span className="fa fa-caret-right" />
        </span>

        {this.renderDetail()}
        {this.renderModal()}
      </div>
    );
  }
}

ExtractFields.propTypes = {
  isOpen: PropTypes.bool,
  column: PropTypes.string,
  onComplete: PropTypes.func
};
