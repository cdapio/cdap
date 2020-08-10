/*
 * Copyright © 2020 Cask Data, Inc.
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

import DataFetcher from 'components/LogViewer/DataFetcher';
import { ILogResponse, LogLevel } from 'components/LogViewer/types';
import { MyPreviewApi } from 'api/preview';
import { Observable } from 'rxjs/Observable';

interface IPreview {
  namespace: string;
  previewId: string;
}

const PREVIEW_LOGS_FILTER =
  'AND .origin=plugin OR MDC:eventType=lifecycle OR MDC:eventType=userLog';

class PreviewDataFetcher implements DataFetcher {
  private namespace;
  private previewId;

  private firstLog;
  private lastLog;
  private includeSystemLogs = false;

  private logLevel = LogLevel.INFO;

  constructor(previewObj: IPreview) {
    this.namespace = previewObj.namespace;
    this.previewId = previewObj.previewId;
  }

  private getFilter = (): string => {
    let filter = `loglevel=${this.logLevel}`;

    if (!this.includeSystemLogs) {
      filter = `${filter} ${PREVIEW_LOGS_FILTER}`;
    }

    return filter;
  };

  private getBaseParams = (): Record<string, string | number> => {
    return {
      namespace: this.namespace,
      previewId: this.previewId,
      max: 50,
      format: 'json',
      filter: this.getFilter(),
    };
  };

  public init = (): Observable<ILogResponse[]> => {
    return this.getLast();
  };

  public getNext = (): Observable<ILogResponse[]> => {
    const params = this.getBaseParams();

    if (this.lastLog) {
      params.fromOffset = this.lastLog.offset;
    }

    return MyPreviewApi.nextLogs(params).map((res = []) => {
      if (res.length > 0) {
        this.lastLog = res[res.length - 1];
      }

      return res;
    });
  };

  public getPrev = (): Observable<ILogResponse[]> => {
    const params = this.getBaseParams();

    if (this.firstLog) {
      params.fromOffset = this.firstLog.offset;
    }

    return MyPreviewApi.prevLogs(params).map((res = []) => {
      if (res.length > 0) {
        this.firstLog = res[0];
      }

      return res;
    });
  };

  public getFirst = (): Observable<ILogResponse[]> => {
    const params = this.getBaseParams();

    return MyPreviewApi.nextLogs(params).map((res = []) => {
      if (res.length > 0) {
        this.firstLog = res[0];
        this.lastLog = res[res.length - 1];
      }

      return res;
    });
  };

  public getLast = (): Observable<ILogResponse[]> => {
    const params = this.getBaseParams();

    return MyPreviewApi.prevLogs(params).map((res = []) => {
      if (res.length > 0) {
        this.firstLog = res[0];
        this.lastLog = res[res.length - 1];
      }

      return res;
    });
  };

  public onLogsTrim = (firstLog: ILogResponse, lastLog: ILogResponse) => {
    this.firstLog = firstLog;
    this.lastLog = lastLog;
  };

  public setIncludeSystemLogs = (includeSystemLogs: boolean): Observable<ILogResponse[]> => {
    this.includeSystemLogs = includeSystemLogs;

    return this.init();
  };

  public getIncludeSystemLogs = (): boolean => {
    return this.includeSystemLogs;
  };

  public setLogLevel = (logLevel: LogLevel): Observable<ILogResponse[]> => {
    this.logLevel = logLevel;

    return this.init();
  };

  public getLogLevel = (): LogLevel => {
    return this.logLevel;
  };

  public getDownloadFileName = (): string => {
    const nameComponents = [this.namespace, this.previewId];

    return nameComponents.join('-');
  };

  public getRawLogsUrl = (): string => {
    const urlComponents = [
      '/v3',
      'namespaces',
      this.namespace,
      'previews',
      this.previewId,
      'logs?escape=false',
    ];

    return urlComponents.join('/');
  };
}

export default PreviewDataFetcher;
