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
import { IProgram, ILogResponse, LogLevel } from 'components/LogViewer/types';
import { MyProgramApi } from 'api/program';
import { Observable } from 'rxjs/Observable';

const PROGRAM_LOGS_FILTER = 'AND .origin=plugin OR .origin=program';
const MAX_LOGS_PER_FETCH = 50;

class ProgramDataFetcher implements DataFetcher {
  private namespace;
  private application;
  private programType;
  private programName;
  private runId;

  private firstLog;
  private lastLog;
  private logFilter;
  private includeSystemLogs = false;

  private logLevel = LogLevel.INFO;

  constructor(programObj: IProgram, logsFilter?: string) {
    this.namespace = programObj.namespace;
    this.application = programObj.application;
    this.programType = programObj.programType;
    this.programName = programObj.programName;
    this.runId = programObj.runId;

    this.logFilter = logsFilter ? logsFilter : PROGRAM_LOGS_FILTER;
  }

  private getFilter = (): string => {
    let filter = `loglevel=${this.logLevel}`;

    if (!this.includeSystemLogs) {
      filter = `${filter} ${this.logFilter}`;
    }

    return filter;
  };

  private getBaseParams = (): Record<string, string | number> => {
    return {
      namespace: this.namespace,
      appId: this.application,
      programType: this.programType,
      programId: this.programName,
      runId: this.runId,
      max: MAX_LOGS_PER_FETCH,
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

    return MyProgramApi.nextLogs(params).map((res = []) => {
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

    return MyProgramApi.prevLogs(params).map((res = []) => {
      if (res.length > 0) {
        this.firstLog = res[0];
      }

      return res;
    });
  };

  public getFirst = (): Observable<ILogResponse[]> => {
    const params = this.getBaseParams();

    return MyProgramApi.nextLogs(params).map((res = []) => {
      if (res.length > 0) {
        this.firstLog = res[0];
        this.lastLog = res[res.length - 1];
      }

      return res;
    });
  };

  public getLast = (): Observable<ILogResponse[]> => {
    const params = this.getBaseParams();

    return MyProgramApi.prevLogs(params).map((res = []) => {
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
    const nameComponents = [
      this.namespace,
      this.application,
      this.programType,
      this.programName,
      this.runId,
    ];

    return nameComponents.join('-');
  };

  public getRawLogsUrl = (): string => {
    const urlComponents = [
      '/v3',
      'namespaces',
      this.namespace,
      'apps',
      this.application,
      this.programType,
      this.programName,
      'runs',
      this.runId,
      'logs?escape=false',
    ];

    return urlComponents.join('/');
  };
}

export default ProgramDataFetcher;
