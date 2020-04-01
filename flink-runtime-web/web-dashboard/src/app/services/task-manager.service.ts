/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { EMPTY, of, ReplaySubject } from 'rxjs';
import { catchError, map } from 'rxjs/operators';
import { BASE_URL } from 'config';
import { TaskManagerListInterface, TaskManagerDetailInterface, TaskManagerLogInterface } from 'interfaces';

@Injectable({
  providedIn: 'root'
})
export class TaskManagerService {
  taskManagerDetail$ = new ReplaySubject<TaskManagerDetailInterface>(1);

  /**
   * Load TM list
   */
  loadManagers() {
    return this.httpClient.get<TaskManagerListInterface>(`${BASE_URL}/taskmanagers`).pipe(
      map(data => data.taskmanagers || []),
      catchError(() => of([]))
    );
  }

  /**
   * Load specify TM
   * @param taskManagerId
   */
  loadManager(taskManagerId: string) {
    return this.httpClient
      .get<TaskManagerDetailInterface>(`${BASE_URL}/taskmanagers/${taskManagerId}`)
      .pipe(catchError(() => EMPTY));
  }

  /**
   * Load TM log list
   * @param taskManagerId
   */
  loadLogList(taskManagerId: string) {
    return this.httpClient
      .get<TaskManagerLogInterface>(`${BASE_URL}/taskmanagers/${taskManagerId}/logs`)
      .pipe(map(data => data.logs));
  }

  /**
   * Load TM log
   * @param taskManagerId
   * @param logName
   * @param hasLogName
   */
  loadLog(taskManagerId: string, logName: string, hasLogName: boolean) {
    let url = '';
    if (hasLogName) {
      url = `${BASE_URL}/taskmanagers/${taskManagerId}/logs/${logName}`;
    } else {
      url = `${BASE_URL}/taskmanagers/${taskManagerId}/log`;
    }
    return this.httpClient
      .get(url, { responseType: 'text', headers: new HttpHeaders().append('Cache-Control', 'no-cache') })
      .pipe(
        map(data => {
          return {
            data,
            url
          };
        })
      );
  }

  constructor(private httpClient: HttpClient) {}
}
