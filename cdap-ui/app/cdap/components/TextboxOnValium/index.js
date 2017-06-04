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

import React, { Component, PropTypes } from 'react';
export default class TextboxOnValium extends Component {
  constructor(props) {
    super(props);
    this.state = {
      textValue: props.value,
      originalValue: props.value
    };
    this.updateTextValue = this.updateTextValue.bind(this);
    this.handleKeyPress = this.handleKeyPress.bind(this);
    this.onBlur = this.onBlur.bind(this);
  }
  componentDidMount() {
    if (this.textboxRef) {
      this.textboxRef.focus();
    }
  }
  updateTextValue(e) {
    let textValue = e.target.value;

    if (!this.props.allowSpace) {
      textValue = textValue.trim();
    }

    this.setState({
      textValue
    }, () => {
      if (this.state.originalValue !== this.state.textValue && this.props.onWarning) {
        let isWarning = this.props.onWarning(this.state.textValue);
        if (isWarning || (!isWarning && this.state.isWarning)) {
          this.setState({
            isWarning
          });
          return;
        }
      }
    });
  }
  onBlur() {
    if (this.state.isWarning) {
      return;
    }
    this.props.onChange(this.state.textValue, this.state.originalValue === this.state.textValue);
  }
  handleKeyPress(e) {
    if (e.nativeEvent.keyCode === 13) {
      this.props.onChange(e.target.value, this.state.originalValue === this.state.textValue, e.nativeEvent.keyCode);
    }
    if (e.nativeEvent.keyCode === 27) {
      this.props.onChange(this.state.originalValue, true, e.nativeEvent.keyCode);
    }
  }
  render() {
    return (
      <input
        className={this.props.className}
        ref={ref => this.textboxRef = ref}
        onBlur={this.onBlur}
        onChange={this.updateTextValue}
        value={this.state.textValue}
        onKeyPress={this.handleKeyPress}
        onKeyUp={this.handleKeyPress}
      />
    );
  }
}

TextboxOnValium.defaultProps = {
  allowSpace: true
};

TextboxOnValium.propTypes = {
  onChange: PropTypes.func,
  value: PropTypes.string,
  onWarning: PropTypes.func,
  className: PropTypes.string,
  allowSpace: PropTypes.bool
};
