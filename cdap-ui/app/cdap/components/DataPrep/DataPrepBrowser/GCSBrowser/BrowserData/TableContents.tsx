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

/**
 * This is here purely for the lack of time. This is mostly a copy-paste of
 * TableContents from S3. Ideally we need a component that accepts data
 * and a callback for each row of the content for lazy loading.
 *
 * Either that or we need to, the very least, extract the common parts
 * between S3 and GCS and use a generic component (Typescipt React<generic> component)
 */
import * as React from 'react';
import { Link } from 'react-router-dom';
import { preventPropagation } from 'services/helpers';
import classnames from 'classnames';
import EmptyMessageContainer from 'components/EmptyMessageContainer';
import {
  humanReadableDate,
  convertBytesToHumanReadable,
  HUMANREADABLESTORAGE_NODECIMAL,
} from 'services/helpers';
import IconSVG from 'components/IconSVG';
import T from 'i18n-react';
import { setGCSPrefix } from 'components/DataPrep/DataPrepBrowser/DataPrepBrowserStore/ActionCreator';
const PREFIX = 'features.DataPrep.DataPrepBrowser.GCSBrowser.BrowserData';

// Lazy load polyfill in safari as InteresectionObservers are not implemented there yet.
(async () => {
  typeof IntersectionObserver === 'undefined'
    ? await import(/* webpackChunkName: "intersection-observer" */ 'intersection-observer')
    : Promise.resolve();
})();

interface IBucketData {
  directory?: boolean;
  name: string;
  size: string;
  wrangle?: boolean;
  type?: string;
  scrollId: number;
  updated?: string;
}

interface ITableContentsProps {
  enableRouting: boolean;
  search: string;
  data: Array<Partial<IBucketData>>;
  onWorkspaceCreate: (file: string) => void;
  prefix: string;
  clearSearch: () => void;
}
interface ITableContentState {
  windowSize: number;
  data: Array<Partial<IBucketData>>;
}

export default class TableContents extends React.PureComponent<
  ITableContentsProps,
  ITableContentState
> {
  private static DEFAULT_WINDOW_SIZE = 100;

  public state: ITableContentState = {
    windowSize: TableContents.DEFAULT_WINDOW_SIZE,
    data: this.props.data.map((d, i) => ({ ...d, scrollId: i })),
  };

  public componentDidMount() {
    Array.from(document.querySelectorAll(`#gcs-buckets-container .row`)).forEach((entry) => {
      this.io.observe(entry);
    });
  }

  public componentDidUpdate() {
    Array.from(document.querySelectorAll(`#gcs-buckets-container .row`)).forEach((entry) => {
      this.io.observe(entry);
    });
  }

  public componentWillReceiveProps(nextProps: Partial<ITableContentsProps>) {
    this.setState({
      data: nextProps.data.map((d, i) => ({ ...d, scrollId: i })),
    });
  }
  private io = new IntersectionObserver(
    (entries) => {
      let lastVisibleElement = this.state.windowSize;
      for (const entry of entries) {
        let id = entry.target.getAttribute('id');
        id = id.split('-').pop();
        const scrollId = parseInt(id, 10);
        if (entry.isIntersecting) {
          lastVisibleElement =
            scrollId + 50 > this.state.windowSize
              ? scrollId + TableContents.DEFAULT_WINDOW_SIZE
              : scrollId;
        }
      }
      if (lastVisibleElement > this.state.windowSize) {
        this.setState({
          windowSize: lastVisibleElement,
        });
      }
    },
    {
      root: document.getElementById('gcs-buckets-container'),
      threshold: [0, 1],
    }
  );

  private getPrefix = (file, prefix) => {
    return file.path ? file.path : `${prefix}${file.name}/`;
  };

  private onClickHandler = (enableRouting, onWorkspaceCreate, file, prefix, e) => {
    if (!file.directory) {
      if (file.wrangle) {
        this.props.onWorkspaceCreate(file);
      }
      preventPropagation(e);
      return false;
    }
    if (enableRouting) {
      return;
    }
    if (file.directory) {
      setGCSPrefix(this.getPrefix(file, prefix));
    }
    preventPropagation(e);
    return false;
  };

  private renderData() {
    const { enableRouting, onWorkspaceCreate, prefix } = this.props;
    const { data } = this.state;
    const ContainerElement = enableRouting ? Link : 'div';
    const pathname = window.location.pathname.replace(/\/cdap/, '');
    if (enableRouting) {
      return (
        <div className="gcs-buckets" id="gcs-buckets-container">
          {data.slice(0, this.state.windowSize).map((file, i) => {
            const lastModified = humanReadableDate(file.updated, true);
            const size =
              convertBytesToHumanReadable(file.size, HUMANREADABLESTORAGE_NODECIMAL, true) || '--';
            let type = file.directory ? T.translate(`${PREFIX}.Content.directory`) : file.type;

            if (file.type === 'UNKNOWN') {
              type = '--';
            }

            return (
              <ContainerElement
                key={file.name}
                className={classnames({ disabled: !file.directory && !file.wrangle })}
                to={`${pathname}?prefix=${this.getPrefix(file, prefix)}`}
                onClick={this.onClickHandler.bind(
                  null,
                  enableRouting,
                  onWorkspaceCreate,
                  file,
                  prefix
                )}
              >
                <div className="row" id={`gcsconnection-${file.scrollId}`}>
                  <div className="col-3">
                    <IconSVG name={file.directory ? 'icon-folder-o' : 'icon-file-o'} />
                    {file.name}
                  </div>
                  <div className="col-3">{type}</div>
                  <div className="col-3">{size}</div>
                  <div className="col-3">{lastModified}</div>
                </div>
              </ContainerElement>
            );
          })}
        </div>
      );
    }

    return (
      <div className="gcs-buckets" id="gcs-buckets-container">
        {data.slice(0, this.state.windowSize).map((file, i) => (
          <ContainerElement
            key={file.name}
            className={classnames({ disabled: !file.directory && !file.wrangle })}
            to={`${pathname}?prefix=${this.getPrefix(file, prefix)}`}
            onClick={this.onClickHandler.bind(null, enableRouting, onWorkspaceCreate, file, prefix)}
          >
            <div className="row" id={`gcsconnection-${file.scrollId}`}>
              <div className="col-12">
                <IconSVG name={file.directory ? 'icon-folder-o' : 'icon-file-o'} />
                {file.name}
              </div>
            </div>
          </ContainerElement>
        ))}
      </div>
    );
  }

  public renderContents() {
    const { search, data, clearSearch } = this.props;
    if (!data.length) {
      return (
        <div className="gcs-buckets empty-message">
          <div className="row">
            <div className="col-12">
              <EmptyMessageContainer searchText={search}>
                <ul>
                  <li>
                    <span className="link-text" onClick={clearSearch}>
                      {T.translate(`features.EmptyMessageContainer.clearLabel`)}
                    </span>
                    <span>
                      {T.translate(`${PREFIX}.Content.EmptymessageContainer.suggestion1`)}
                    </span>
                  </li>
                </ul>
              </EmptyMessageContainer>
            </div>
          </div>
        </div>
      );
    }
    return this.renderData();
  }
  public render() {
    return <div className="gcs-content-body">{this.renderContents()}</div>;
  }
}
