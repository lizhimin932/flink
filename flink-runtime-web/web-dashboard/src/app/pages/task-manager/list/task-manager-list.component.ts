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
import { ActivatedRoute, Router } from '@angular/router';
import { Subject } from 'rxjs';
import { mergeMap, takeUntil } from 'rxjs/operators';

import { NzTableSortFn } from 'ng-zorro-antd/table/src/table.types';

import { TaskmanagersItem } from 'interfaces';
import { StatusService, TaskManagerService } from 'services';

function createSortFn(selector: (item: TaskmanagersItem) => number): NzTableSortFn<TaskmanagersItem> {
  return (pre, next) => (selector(pre) > selector(next) ? 1 : -1);
}

@Component({
  selector: 'flink-task-manager-list',
  templateUrl: './task-manager-list.component.html',
  styleUrls: ['./task-manager-list.component.less'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class TaskManagerListComponent implements OnInit, OnDestroy {
  public readonly trackById = (_: number, node: TaskmanagersItem): string => node.id;

  public readonly sortIdFn = createSortFn(item => item.id);
  public readonly sortDataPortFn = createSortFn(item => item.dataPort);
  public readonly sortHeartBeatFn = createSortFn(item => item.timeSinceLastHeartbeat);
  public readonly sortSlotsNumberFn = createSortFn(item => item.slotsNumber);
  public readonly sortFreeSlotsFn = createSortFn(item => item.freeSlots);
  public readonly sortCpuCoresFn = createSortFn(item => item.hardware?.cpuCores);
  public readonly sortPhysicalMemoryFn = createSortFn(item => item.hardware?.physicalMemory);
  public readonly sortFreeMemoryFn = createSortFn(item => item.hardware?.freeMemory);
  public readonly sortManagedMemoryFn = createSortFn(item => item.hardware?.managedMemory);

  public listOfTaskManager: TaskmanagersItem[] = [];
  public isLoading = true;
  public sortName: string;
  public sortValue: string;

  private readonly destroy$ = new Subject<void>();

  public navigateTo(taskManager: TaskmanagersItem): void {
    this.router.navigate([taskManager.id, 'metrics'], { relativeTo: this.activatedRoute }).then();
  }

  constructor(
    private readonly cdr: ChangeDetectorRef,
    private readonly statusService: StatusService,
    private readonly taskManagerService: TaskManagerService,
    private readonly router: Router,
    private readonly activatedRoute: ActivatedRoute
  ) {}

  public ngOnInit(): void {
    this.statusService.refresh$
      .pipe(
        takeUntil(this.destroy$),
        mergeMap(() => this.taskManagerService.loadManagers())
      )
      .subscribe(
        data => {
          this.isLoading = false;
          this.listOfTaskManager = data;
          this.cdr.markForCheck();
        },
        () => {
          this.isLoading = false;
          this.cdr.markForCheck();
        }
      );
  }

  public ngOnDestroy(): void {
    this.destroy$.next();
    this.destroy$.complete();
  }
}
