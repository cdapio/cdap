/*
 * Copyright © 2018 Cask Data, Inc.
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

import React, { Component } from 'react';
import PropTypes from 'prop-types';
import { DEFAULT_WIDGET_PROPS } from 'components/AbstractWidget';
import { MySecureKeyApi } from 'api/securekey';
import { getCurrentNamespace } from 'services/NamespaceStore';
import IconSVG from 'components/IconSVG';
import classnames from 'classnames';
import T from 'i18n-react';
import { objectQuery } from 'services/helpers';
import { SECURE_KEY_PREFIX, SECURE_KEY_SUFFIX, SYSTEM_NAMESPACE } from 'services/global-constants';
import Mousetrap from 'mousetrap';
import { Observable } from 'rxjs/Observable';
import { WIDGET_PROPTYPES } from 'components/AbstractWidget/constants';
require('./SecureKeyTextarea.scss');

const PREFIX = 'features.AbstractWidget.SecureKeyTextarea';

export default class SecureKeyTextarea extends Component {
  static propTypes = {
    ...WIDGET_PROPTYPES,
    inputTextType: PropTypes.oneOf(['textarea', 'text', 'password']),
    dataCy: PropTypes.string,
  };
  static defaultProps = DEFAULT_WIDGET_PROPS;

  documentClick$ = null;
  containerRef = null;

  state = {
    secureKeys: [],
    expanded: false,
    customEntry: false,
    customEntryText: '',
  };

  componentDidMount() {
    const namespace = objectQuery(this.props, 'extraConfig', 'namespace') || getCurrentNamespace();

    if (namespace === SYSTEM_NAMESPACE) {
      return;
    }

    const params = {
      namespace,
    };

    MySecureKeyApi.list(params).subscribe((keys) => {
      this.setState({
        secureKeys: keys,
      });
    });
  }

  setEventListenersForToggle = () => {
    if (this.documentClick$) {
      return;
    }
    Mousetrap.bind('esc', this.toggleExpand);
    this.documentClick$ = Observable.fromEvent(document, 'click').subscribe((e) => {
      /**
       * eehhh wat?
       * There will be a case where when the user clicks on the "Specify a different secure key"
       * button we show a enter a custom secure key. During this transition the helper text(button)
       * is already removed when the event finally comes here. So even though e.target shows a
       * valid element it is already removed from the DOM. Hence this check if is actually present
       * in the DOM.
       */
      if (!this.containerRef.contains(e.target) && document.body.contains(e.target)) {
        this.toggleExpand();
      }
    });
  };

  unsetEventListenersForToggle = () => {
    Mousetrap.unbind('esc');
    if (this.documentClick$) {
      this.documentClick$.unsubscribe();
      this.documentClick$ = null;
    }
  };

  toggleExpand = () => {
    if (!this.state.expanded) {
      this.setEventListenersForToggle();
    } else {
      this.unsetEventListenersForToggle();
    }
    this.setState({
      expanded: !this.state.expanded,
      customEntry: false,
      customEntryText: '',
    });
  };

  toggleCustomEntry = () => {
    this.setState({
      customEntry: !this.state.customEntry,
      customEntryText: '',
    });
  };

  handleKeyPress = (e) => {
    const keyText = this.state.customEntryText;

    if (e.nativeEvent.keyCode !== 13 || keyText.length === 0) {
      return;
    }

    this.onSecureKeySelect(keyText);
  };

  onSecureKeySelect = (key) => {
    const secureKey = `${SECURE_KEY_PREFIX}${key}${SECURE_KEY_SUFFIX}`;

    this.props.onChange(secureKey);
    this.toggleExpand();
  };

  handleCustomEntryChange = (e) => {
    this.setState({
      customEntryText: e.target.value,
    });
  };

  handleOnInputChange = (e) => {
    const { value } = e.target;
    if (this.props.onChange) {
      this.props.onChange(value);
    }
  };

  renderCustomEntry = () => {
    const helperText = (
      <div className="helper-text text-center" onClick={this.toggleCustomEntry}>
        {T.translate(`${PREFIX}.customEntryHelper`)}
      </div>
    );

    const textbox = (
      <input
        type="text"
        className="form-control"
        value={this.state.customEntryText}
        onChange={this.handleCustomEntryChange}
        onKeyPress={this.handleKeyPress}
        placeholder={T.translate(`${PREFIX}.customEntryPlaceholder`)}
      />
    );

    return (
      <div className={classnames('custom-entry-row', { expanded: this.state.customEntry })}>
        {this.state.customEntry ? textbox : helperText}
      </div>
    );
  };

  renderSecureKeyContent = () => {
    if (!this.state.expanded) {
      return null;
    }

    return (
      <div className="secure-key-content">
        <div className="keys-list">
          {this.state.secureKeys.map((key) => {
            return (
              <div
                key={key.name}
                className="secure-key-row"
                onClick={this.onSecureKeySelect.bind(this, key.name)}
              >
                {key.name}
              </div>
            );
          })}
        </div>

        {this.renderCustomEntry()}
      </div>
    );
  };

  renderSecureKey = () => {
    const text = <span className="title">{T.translate(`${PREFIX}.title`)}</span>;

    return (
      <div className={classnames('secure-key', { expanded: this.state.expanded })}>
        <div className="top-part">
          <IconSVG name="icon-shield" onClick={this.toggleExpand} />

          {this.state.expanded ? text : null}
          {this.state.expanded ? <IconSVG name="icon-close" onClick={this.toggleExpand} /> : null}
        </div>

        {this.renderSecureKeyContent()}
      </div>
    );
  };

  renderInput = () => {
    if (this.props.inputTextType === 'textarea') {
      return (
        <textarea
          className="form-control raw-text-input"
          onChange={this.handleOnInputChange}
          value={this.props.value}
          data-cy={this.props.dataCy}
          {...this.props.widgetProps}
        />
      );
    }

    return (
      <input
        type={this.props.inputTextType}
        className="form-control raw-text-input"
        onChange={this.handleOnInputChange}
        value={this.props.value}
        data-cy={this.props.dataCy}
        {...this.props.widgetProps}
      />
    );
  };

  render() {
    return (
      <div className="secure-key-textarea-widget" ref={(ref) => (this.containerRef = ref)}>
        {this.renderInput()}
        {this.renderSecureKey()}
      </div>
    );
  }
}
