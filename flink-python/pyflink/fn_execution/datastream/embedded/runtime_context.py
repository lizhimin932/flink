################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from pyflink.datastream import RuntimeContext
from pyflink.datastream.state import (AggregatingStateDescriptor, AggregatingState,
                                      ReducingStateDescriptor, ReducingState, MapStateDescriptor,
                                      MapState, ListStateDescriptor, ListState,
                                      ValueStateDescriptor, ValueState)
from pyflink.fn_execution.datastream.embedded.state_impl import (ValueStateImpl, ListStateImpl,
                                                                 MapStateImpl, ReducingStateImpl,
                                                                 AggregatingStateImpl)
from pyflink.fn_execution.embedded.converters import from_type_info
from pyflink.fn_execution.embedded.java_utils import to_java_state_descriptor


class StreamingRuntimeContext(RuntimeContext):
    def __init__(self, runtime_context, job_parameters, keyed_state_backend=None):
        self._runtime_context = runtime_context
        self._job_parameters = job_parameters
        if keyed_state_backend:
            self._keyed_state_backend = keyed_state_backend

    def get_task_name(self) -> str:
        """
        Returns the name of the task in which the UDF runs, as assigned during plan construction.
        """
        return self._runtime_context.getTaskName()

    def get_number_of_parallel_subtasks(self) -> int:
        """
        Gets the parallelism with which the parallel task runs.
        """
        return self._runtime_context.getNumberOfParallelSubtasks()

    def get_max_number_of_parallel_subtasks(self) -> int:
        """
        Gets the number of max-parallelism with which the parallel task runs.
        """
        return self._runtime_context.getMaxNumberOfParallelSubtasks()

    def get_index_of_this_subtask(self) -> int:
        """
        Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
        parallelism-1 (parallelism as returned by
        :func:`~RuntimeContext.get_number_of_parallel_subtasks`).
        """
        return self._runtime_context.getIndexOfThisSubtask()

    def get_attempt_number(self) -> int:
        """
        Gets the attempt number of this parallel subtask. First attempt is numbered 0.
        """
        return self._runtime_context.getAttemptNumber()

    def get_task_name_with_subtasks(self) -> str:
        """
        Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)",
        where 3 would be (:func:`~RuntimeContext.get_index_of_this_subtask` + 1), and 6 would be
        :func:`~RuntimeContext.get_number_of_parallel_subtasks`.
        """
        return self._runtime_context.getTaskNameWithSubtasks()

    def get_job_parameter(self, key: str, default_value: str):
        """
        Gets the global job parameter value associated with the given key as a string.
        """
        return self._job_parameters[key] if key in self._job_parameters else default_value

    def get_metrics_group(self):
        return self._runtime_context.getMetricGroup()

    def get_state(self, state_descriptor: ValueStateDescriptor) -> ValueState:
        if hasattr(self, '_keyed_state_backend'):
            return self._keyed_state_backend.get_value_state(state_descriptor)
        else:
            return ValueStateImpl(
                self._runtime_context.getState(to_java_state_descriptor(state_descriptor)),
                from_type_info(state_descriptor.type_info))

    def get_list_state(self, state_descriptor: ListStateDescriptor) -> ListState:
        if hasattr(self, '_keyed_state_backend'):
            return self._keyed_state_backend.get_list_state(state_descriptor)
        else:
            return ListStateImpl(
                self._runtime_context.getListState(to_java_state_descriptor(state_descriptor)),
                from_type_info(state_descriptor.type_info))

    def get_map_state(self, state_descriptor: MapStateDescriptor) -> MapState:
        if hasattr(self, '_keyed_state_backend'):
            return self._keyed_state_backend.get_map_state(state_descriptor)
        else:
            return MapStateImpl(
                self._runtime_context.getMapState(to_java_state_descriptor(state_descriptor)),
                from_type_info(state_descriptor.type_info))

    def get_reducing_state(self, state_descriptor: ReducingStateDescriptor) -> ReducingState:
        if hasattr(self, '_keyed_state_backend'):
            return self._keyed_state_backend.get_reducing_state(state_descriptor)
        else:
            return ReducingStateImpl(
                self._runtime_context.getState(to_java_state_descriptor(state_descriptor)),
                from_type_info(state_descriptor.type_info),
                state_descriptor.get_reduce_function())

    def get_aggregating_state(self,
                              state_descriptor: AggregatingStateDescriptor) -> AggregatingState:
        if hasattr(self, '_keyed_state_backend'):
            return self._keyed_state_backend.get_aggregating_state(state_descriptor)
        else:
            return AggregatingStateImpl(
                self._runtime_context.getState(to_java_state_descriptor(state_descriptor)),
                from_type_info(state_descriptor.type_info),
                state_descriptor.get_agg_function())

    @staticmethod
    def of(runtime_context, job_parameters, keyed_state_backend=None):
        return StreamingRuntimeContext(runtime_context, job_parameters, keyed_state_backend)
