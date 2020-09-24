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
from functools import reduce
from itertools import chain

from apache_beam.runners.worker import bundle_processor
from apache_beam.runners.worker import operation_specs
from apache_beam.runners.worker.operations import Operation
from apache_beam.utils.windowed_value import WindowedValue

from pyflink.fn_execution import flink_fn_execution_pb2, operation_utils
from pyflink.metrics.metricbase import GenericMetricGroup
from pyflink.table import FunctionContext


class StatelessFunctionOperation(Operation):
    """
    Base class of stateless function operation that will execute ScalarFunction or TableFunction for
    each input element.
    """

    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(StatelessFunctionOperation, self).__init__(name, spec, counter_factory, sampler)
        self.consumer = consumers['output'][0]
        self._value_coder_impl = self.consumer.windowed_coder.wrapped_value_coder.get_impl()

        self.func, self.user_defined_funcs = self.generate_func(self.spec.serialized_fn.udfs)
        self._metric_enabled = self.spec.serialized_fn.metric_enabled
        self.base_metric_group = None
        if self._metric_enabled:
            self.base_metric_group = GenericMetricGroup(None, None)
        for user_defined_func in self.user_defined_funcs:
            user_defined_func.open(FunctionContext(self.base_metric_group))

    def setup(self):
        super(StatelessFunctionOperation, self).setup()

    def start(self):
        with self.scoped_start_state:
            super(StatelessFunctionOperation, self).start()

    def finish(self):
        with self.scoped_finish_state:
            super(StatelessFunctionOperation, self).finish()
            self._update_gauge(self.base_metric_group)

    def needs_finalization(self):
        return False

    def reset(self):
        super(StatelessFunctionOperation, self).reset()

    def teardown(self):
        with self.scoped_finish_state:
            for user_defined_func in self.user_defined_funcs:
                user_defined_func.close()

    def progress_metrics(self):
        metrics = super(StatelessFunctionOperation, self).progress_metrics()
        metrics.processed_elements.measured.output_element_counts.clear()
        tag = None
        receiver = self.receivers[0]
        metrics.processed_elements.measured.output_element_counts[
            str(tag)] = receiver.opcounter.element_counter.value()
        return metrics

    def process(self, o: WindowedValue):
        with self.scoped_process_state:
            output_stream = self.consumer.output_stream
            self._value_coder_impl.encode_to_stream(self.func(o.value), output_stream, True)
            output_stream.maybe_flush()

    def monitoring_infos(self, transform_id, tag_to_pcollection_id):
        """
        Only pass user metric to Java
        :param tag_to_pcollection_id: useless for user metric
        """
        return super().user_monitoring_infos(transform_id)

    def generate_func(self, udfs) -> tuple:
        pass

    @staticmethod
    def _update_gauge(base_metric_group):
        if base_metric_group is not None:
            for name in base_metric_group._flink_gauge:
                flink_gauge = base_metric_group._flink_gauge[name]
                beam_gauge = base_metric_group._beam_gauge[name]
                beam_gauge.set(flink_gauge())
            for sub_group in base_metric_group._sub_groups:
                StatelessFunctionOperation._update_gauge(sub_group)


class ScalarFunctionOperation(StatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(ScalarFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    def generate_func(self, udfs):
        """
        Generates a lambda function based on udfs.
        :param udfs: a list of the proto representation of the Python :class:`ScalarFunction`
        :return: the generated lambda function
        """
        scalar_functions, variable_dict, user_defined_funcs = reduce(
            lambda x, y: (
                ','.join([x[0], y[0]]),
                dict(chain(x[1].items(), y[1].items())),
                x[2] + y[2]),
            [operation_utils.extract_user_defined_function(udf) for udf in udfs])
        mapper = eval('lambda value: [%s]' % scalar_functions, variable_dict)
        return lambda it: map(mapper, it), user_defined_funcs


class TableFunctionOperation(StatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(TableFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    def generate_func(self, udtfs):
        """
        Generates a lambda function based on udtfs.
        :param udtfs: a list of the proto representation of the Python :class:`TableFunction`
        :return: the generated lambda function
        """
        table_function, variable_dict, user_defined_funcs = \
            operation_utils.extract_user_defined_function(udtfs[0])
        mapper = eval('lambda value: %s' % table_function, variable_dict)
        return lambda it: map(mapper, it), user_defined_funcs


class DataStreamStatelessFunctionOperation(StatelessFunctionOperation):

    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(DataStreamStatelessFunctionOperation, self).__init__(name, spec, counter_factory,
                                                                   sampler, consumers)

    def generate_func(self, udfs):
        func = operation_utils.extract_data_stream_stateless_funcs(udfs=udfs)
        return lambda it: map(func, it), []


class PandasAggregateFunctionOperation(StatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(PandasAggregateFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)

    def generate_func(self, udfs):
        pandas_functions, variable_dict, user_defined_funcs = reduce(
            lambda x, y: (
                ','.join([x[0], y[0]]),
                dict(chain(x[1].items(), y[1].items())),
                x[2] + y[2]),
            [operation_utils.extract_user_defined_function(udf, True) for udf in udfs])
        variable_dict['wrap_pandas_result'] = operation_utils.wrap_pandas_result
        mapper = eval('lambda value: wrap_pandas_result([%s])' % pandas_functions, variable_dict)
        return lambda it: map(mapper, it), user_defined_funcs


class PandasBatchOverWindowAggregateFunctionOperation(StatelessFunctionOperation):
    def __init__(self, name, spec, counter_factory, sampler, consumers):
        super(PandasBatchOverWindowAggregateFunctionOperation, self).__init__(
            name, spec, counter_factory, sampler, consumers)
        self.windows = [window for window in self.spec.serialized_fn.windows]
        # Set a serial number for each over window to indicate which bounded range over window
        # it is.
        self.window_to_bounded_range_window_index = [-1 for _ in range(len(self.windows))]
        # Whether the specified position window is a bounded range window.
        self.window_is_bounded_range_type = []
        window_types = flink_fn_execution_pb2.OverWindow

        bounded_range_window_nums = 0
        for i, window in enumerate(self.windows):
            window_type = window.window_type
            if (window_type is window_types.RANGE_UNBOUNDED_PRECEDING) or (
                    window_type is window_types.RANGE_UNBOUNDED_FOLLOWING) or (
                    window_type is window_types.RANGE_SLIDING):
                self.window_to_bounded_range_window_index[i] = bounded_range_window_nums
                self.window_is_bounded_range_type.append(True)
                bounded_range_window_nums += 1
            else:
                self.window_is_bounded_range_type.append(False)

    def generate_func(self, udfs):
        user_defined_funcs = []
        self.window_indexes = []
        self.mapper = []
        for udf in udfs:
            pandas_agg_function, variable_dict, user_defined_func, window_index = \
                operation_utils.extract_over_window_user_defined_function(udf)
            user_defined_funcs.extend(user_defined_func)
            self.window_indexes.append(window_index)
            self.mapper.append(eval('lambda value: %s' % pandas_agg_function, variable_dict))
        return self.wrap_over_window_function, user_defined_funcs

    def wrap_over_window_function(self, it):
        import pandas as pd
        window_types = flink_fn_execution_pb2.OverWindow
        for boundaries_series in it:
            series = boundaries_series[-1]
            # the row number of the arrow format data
            rows_count = len(series[0])
            results = []
            # loop every agg func
            for i in range(len(self.window_indexes)):
                window_index = self.window_indexes[i]
                # the over window which the agg function belongs to
                window = self.windows[window_index]
                window_type = window.window_type
                func = self.mapper[i]
                result = []
                if self.window_is_bounded_range_type[window_index]:
                    window_boundaries = boundaries_series[
                        self.window_to_bounded_range_window_index[window_index]]
                    if window_type is window_types.RANGE_UNBOUNDED_PRECEDING:
                        # range unbounded preceding window
                        for j in range(rows_count):
                            end = window_boundaries[j]
                            series_slices = [s.iloc[:end] for s in series]
                            result.append(func(series_slices))
                    elif window_type is window_types.RANGE_UNBOUNDED_FOLLOWING:
                        # range unbounded following window
                        for j in range(rows_count):
                            start = window_boundaries[j]
                            series_slices = [s.iloc[start:] for s in series]
                            result.append(func(series_slices))
                    else:
                        # range sliding window
                        for j in range(rows_count):
                            start = window_boundaries[j * 2]
                            end = window_boundaries[j * 2 + 1]
                            series_slices = [s.iloc[start:end] for s in series]
                            result.append(func(series_slices))
                else:
                    # unbounded range window or unbounded row window
                    if (window_type is window_types.RANGE_UNBOUNDED) or (
                            window_type is window_types.ROW_UNBOUNDED):
                        series_slices = [s.iloc[:] for s in series]
                        func_result = func(series_slices)
                        result = [func_result for _ in range(rows_count)]
                    elif window_type is window_types.ROW_UNBOUNDED_PRECEDING:
                        # row unbounded preceding window
                        window_end = window.upper_boundary
                        for j in range(rows_count):
                            end = min(j + window_end + 1, rows_count)
                            series_slices = [s.iloc[: end] for s in series]
                            result.append(func(series_slices))
                    elif window_type is window_types.ROW_UNBOUNDED_FOLLOWING:
                        # row unbounded following window
                        window_start = window.lower_boundary
                        for j in range(rows_count):
                            start = max(j + window_start, 0)
                            series_slices = [s.iloc[start: rows_count] for s in series]
                            result.append(func(series_slices))
                    else:
                        # row sliding window
                        window_start = window.lower_boundary
                        window_end = window.upper_boundary
                        for j in range(rows_count):
                            start = max(j + window_start, 0)
                            end = min(j + window_end + 1, rows_count)
                            series_slices = [s.iloc[start: end] for s in series]
                            result.append(func(series_slices))
                results.append(pd.Series(result))
            yield results

@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.SCALAR_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_scalar_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, ScalarFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.TABLE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_table_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, TableFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.DATA_STREAM_STATELESS_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedDataStreamFunctions)
def create_data_stream_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, DataStreamStatelessFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.PANDAS_AGGREGATE_FUNCTION_URN, flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_aggregate_function(factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter, PandasAggregateFunctionOperation)


@bundle_processor.BeamTransformFactory.register_urn(
    operation_utils.PANDAS_BATCH_OVER_WINDOW_AGGREGATE_FUNCTION_URN,
    flink_fn_execution_pb2.UserDefinedFunctions)
def create_pandas_over_window_aggregate_function(
        factory, transform_id, transform_proto, parameter, consumers):
    return _create_user_defined_function_operation(
        factory, transform_proto, consumers, parameter,
        PandasBatchOverWindowAggregateFunctionOperation)


def _create_user_defined_function_operation(factory, transform_proto, consumers, udfs_proto,
                                            operation_cls):
    output_tags = list(transform_proto.outputs.keys())
    output_coders = factory.get_output_coders(transform_proto)
    spec = operation_specs.WorkerDoFn(
        serialized_fn=udfs_proto,
        output_tags=output_tags,
        input=None,
        side_inputs=None,
        output_coders=[output_coders[tag] for tag in output_tags])

    return operation_cls(
        transform_proto.unique_name,
        spec,
        factory.counter_factory,
        factory.state_sampler,
        consumers)
