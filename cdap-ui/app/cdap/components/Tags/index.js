/*
 * Copyright © 2017-2018 Cask Data, Inc.
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

import PropTypes from 'prop-types';

import React, { Component } from 'react';
import {MyMetadataApi} from 'api/metadata';
import isObject from 'lodash/isObject';
import Mousetrap from 'mousetrap';
import NamespaceStore from 'services/NamespaceStore';
import {convertEntityTypeToApi} from 'services/entity-type-api-converter';
import Tag from 'components/Tags/Tag';
require('./Tags.scss');
import T from 'i18n-react';

export default class Tags extends Component {
  static defaultProps = {
    showCountLabel: true,
    isNativeLink: false
  };

  static propTypes = {
    entity: PropTypes.object,
    showCountLabel: PropTypes.bool,
    isNativeLink: PropTypes.bool
  };

  state = {
    systemTags: [],
    userTags: [],
    showInputField: false,
    loading: false,
    currentInputTag: '',
  };

  params = {
    namespace: NamespaceStore.getState().selectedNamespace,
    entityType: convertEntityTypeToApi(this.props.entity.type),
    entityId: this.props.entity.id,
  };

  subscriptions = [];

  componentWillMount() {
    Mousetrap.bind('return', this.addTag);
    Mousetrap.bind('escape', this.closeInputFieldIfEmpty);

    let systemParams = Object.assign({}, this.params, { scope: 'SYSTEM' });
    let userParams = Object.assign({}, this.params, { scope: 'USER' });

    this.setState({
      loading: true
    });

    let getTagsSubscription = MyMetadataApi
      .getTags(systemParams)
      .combineLatest(MyMetadataApi.getTags(userParams))
      .subscribe((res) => {
        this.setState({
          systemTags: res[0].sort(),
          userTags: res[1].sort(),
          loading: false
        });
      }, (err) => {
        this.setState({
          error: isObject(err) ? err.response : err
        });
      });

    this.subscriptions.push(getTagsSubscription);
  }

  componentWillUnmount() {
    Mousetrap.unbind('return');
    Mousetrap.unbind('escape');

    this.subscriptions.map(subscriber => subscriber.unsubscribe());
  }

  toggleInputField = () => {
    if (!this.state.loading) {
      if (this.state.showInputField) {
        this.setState({
          currentInputTag: '',
          error: false
        });
      }
      this.setState({
        showInputField: !this.state.showInputField
      });
    }
  };

  closeInputFieldIfEmpty = () => {
    if (this.state.currentInputTag === '' && this.state.showInputField) {
      this.setState({
        showInputField: false,
        error: false
      });
    }
  };

  onInputTagChange = (e) => {
    this.setState({
      currentInputTag: e.target.value
    });
  };

  fetchUserTags() {
    let params = Object.assign({}, this.params, { scope: 'USER' });

    let fetchTagsSubscription = MyMetadataApi
      .getTags(params)
      .subscribe((res) => {
        this.setState({
          userTags: res.sort(),
          loading: false
        });
        if (this.state.showInputField) {
          this.toggleInputField();
        }
      }, (err) => {
        this.setState({
          error: isObject(err) ? err.response : err,
          loading: false
        });
      });

    this.subscriptions.push(fetchTagsSubscription);
  }

  addTag = () => {
    if (this.state.currentInputTag !== '') {
      this.setState({
        loading: true
      });
      let addTagsSubscription = MyMetadataApi
        .addTags(this.params, [this.state.currentInputTag])
        .subscribe(() => {
          this.fetchUserTags();
        }, (err) => {
          this.setState({
            error: isObject(err) ? err.response : err,
            loading: false
          });
        });

      this.subscriptions.push(addTagsSubscription);
    }
  };

  deleteTag(tag, event) {
    event.preventDefault();
    event.stopPropagation();
    event.nativeEvent.stopImmediatePropagation();

    let params = Object.assign({}, this.params, { key: tag });

    let deleteTagsSubscription = MyMetadataApi
      .deleteTags(params)
      .subscribe(() => {
        this.fetchUserTags();
      }, (err) => {
        this.setState({
          error: isObject(err) ? err.response : err
        });
      });

    this.subscriptions.push(deleteTagsSubscription);
  }

  renderSystemTags() {
    return (
      <span>
        {
          this.state.systemTags.map(tag => {
            return (
              <Tag
                value={tag}
                scope='SYSTEM'
                isNativeLink={this.props.isNativeLink}
              />
            );
          })
        }
      </span>
    );
  }

  renderUserTags() {
    return (
      <span>
        {
          this.state.userTags.map(tag => {
            return (
              <Tag
                value={tag}
                onDelete={this.deleteTag.bind(this, tag)}
                scope='USER'
                isNativeLink={this.props.isNativeLink}
              />
            );
          })
        }
      </span>
    );
  }

  renderInputField() {
    return (
      <span>
        <input
          type="text"
          className="tag-input form-control mousetrap"
          value={this.state.currentInputTag}
          onChange={this.onInputTagChange}
          onBlur={this.toggleInputField}
          autoFocus={true}
          disabled={this.state.loading ? 'disabled' : null}
        />
        {
          this.state.loading ?
            <span className="fa fa-lg fa-spinner fa-spin" />
          :
            null
        }
      </span>
    );
  }

  renderPlusButton() {
    return (
      <span
        className="btn btn-primary plus-button-container"
        onClick={this.toggleInputField}
      >
        <span className="text-white">+</span>
      </span>
    );
  }

  render() {
    let tagsCount = this.state.systemTags.length + this.state.userTags.length;
    return (
      <div className="tags-holder">
        {
          this.props.showCountLabel ?
            <strong> {T.translate('features.Tags.label')}({tagsCount}): </strong>
          :
            null
        }
        {
          !tagsCount && !this.state.loading ?
            <i>{T.translate('features.Tags.notags')}</i>
          :
            null
        }
        {
          this.state.loading ?
            <span className="fa fa-lg fa-spinner fa-spin" />
          :
            null
        }
        <span>
          {this.renderSystemTags()}
          {this.renderUserTags()}
          {
            this.state.showInputField ?
              this.renderInputField()
            :
              this.renderPlusButton()
          }
          {
            this.state.error ?
              <span className="text-danger">
                <p>{this.state.error}</p>
              </span>
            :
              null
          }

        </span>
      </div>
    );
  }
}
