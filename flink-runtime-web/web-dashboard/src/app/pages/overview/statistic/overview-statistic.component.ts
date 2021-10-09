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

import { ChangeDetectionStrategy, ChangeDetectorRef, Component, OnDestroy, OnInit } from '@angular/core';
import { Subject } from 'rxjs';
import { flatMap, takeUntil } from 'rxjs/operators';

import { OverviewInterface } from 'interfaces';
import { OverviewService, StatusService } from 'services';

@Component({
  selector: 'flink-overview-statistic',
  templateUrl: './overview-statistic.component.html',
  styleUrls: ['./overview-statistic.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class OverviewStatisticComponent implements OnInit, OnDestroy {
  statistic: OverviewInterface | null;
  destroy$ = new Subject();

  constructor(
    private statusService: StatusService,
    private overviewService: OverviewService,
    private cdr: ChangeDetectorRef
  ) {}

  ngOnInit(): void {
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        flatMap(() => this.overviewService.loadOverview())
      )
      .subscribe(data => {
        this.statistic = data;
        this.cdr.markForCheck();
      });
  }

  ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
