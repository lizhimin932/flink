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
import datetime
import decimal
import os
import uuid

from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.datastream import TimeCharacteristic, RuntimeContext
from pyflink.datastream.data_stream import DataStream
from pyflink.datastream.functions import CoMapFunction, CoFlatMapFunction, AggregateFunction, \
    ReduceFunction
from pyflink.datastream.functions import FilterFunction, ProcessFunction, KeyedProcessFunction
from pyflink.datastream.functions import KeySelector
from pyflink.datastream.functions import MapFunction, FlatMapFunction
from pyflink.datastream.state import ValueStateDescriptor, ListStateDescriptor, \
    MapStateDescriptor, ReducingStateDescriptor, ReducingState, AggregatingState, \
    AggregatingStateDescriptor
from pyflink.datastream.tests.test_util import DataStreamTestSinkFunction
from pyflink.java_gateway import get_gateway
from pyflink.testing.test_case_utils import invoke_java_object_method, \
    PyFlinkBatchTestCase, PyFlinkStreamingTestCase


class DataStreamTests(object):

    def setUp(self) -> None:
        super(DataStreamTests, self).setUp()
        config = invoke_java_object_method(
            self.env._j_stream_execution_environment, "getConfiguration")
        config.setString("akka.ask.timeout", "20 s")
        self.test_sink = DataStreamTestSinkFunction()

    def tearDown(self) -> None:
        self.test_sink.clear()

    def test_reduce_function_without_data_types(self):
        ds = self.env.from_collection([(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')],
                                      type_info=Types.ROW([Types.INT(), Types.STRING()]))
        ds.key_by(lambda a: a[1]) \
          .reduce(lambda a, b: Row(a[0] + b[0], b[1])) \
          .add_sink(self.test_sink)
        self.env.execute('reduce_function_test')
        result = self.test_sink.get_results()
        expected = ["+I[1, a]", "+I[3, a]", "+I[6, a]", "+I[4, b]"]
        expected.sort()
        result.sort()
        self.assertEqual(expected, result)

    def test_map_function_without_data_types(self):
        self.env.set_parallelism(1)
        ds = self.env.from_collection([('ab', decimal.Decimal(1)),
                                       ('bdc', decimal.Decimal(2)),
                                       ('cfgs', decimal.Decimal(3)),
                                       ('deeefg', decimal.Decimal(4))],
                                      type_info=Types.ROW([Types.STRING(), Types.BIG_DEC()]))
        ds.map(MyMapFunction()).add_sink(self.test_sink)
        self.env.execute('map_function_test')
        results = self.test_sink.get_results(True)
        expected = ["<Row('ab', 2, Decimal('1'))>", "<Row('bdc', 3, Decimal('2'))>",
                    "<Row('cfgs', 4, Decimal('3'))>", "<Row('deeefg', 6, Decimal('4'))>"]
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_map_function_with_data_types(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.TUPLE([Types.STRING(), Types.INT()]))

        def map_func(value):
            result = Row(value[0], len(value[0]), value[1])
            return result

        ds.map(map_func, output_type=Types.ROW([Types.STRING(), Types.INT(), Types.INT()]))\
            .add_sink(self.test_sink)
        self.env.execute('map_function_test')
        results = self.test_sink.get_results(False)
        expected = ['+I[ab, 2, 1]', '+I[bdc, 3, 2]', '+I[cfgs, 4, 3]', '+I[deeefg, 6, 4]']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_co_map_function_without_data_types(self):
        self.env.set_parallelism(1)
        ds1 = self.env.from_collection([(1, 1), (2, 2), (3, 3)],
                                       type_info=Types.ROW([Types.INT(), Types.INT()]))
        ds2 = self.env.from_collection([("a", "a"), ("b", "b"), ("c", "c")],
                                       type_info=Types.ROW([Types.STRING(), Types.STRING()]))
        ds1.connect(ds2).map(MyCoMapFunction()).add_sink(self.test_sink)
        self.env.execute('co_map_function_test')
        results = self.test_sink.get_results(True)
        expected = ['2', '3', '4', 'a', 'b', 'c']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_connected_streams_with_dependency(self):
        python_file_dir = os.path.join(self.tempdir, "python_file_dir_" + str(uuid.uuid4()))
        os.mkdir(python_file_dir)
        python_file_path = os.path.join(python_file_dir, "test_stream_dependency_manage_lib.py")
        with open(python_file_path, 'w') as f:
            f.write("def add_two(a):\n    return a + 2")

        class TestCoMapFunction(CoMapFunction):

            def map1(self, value):
                from test_stream_dependency_manage_lib import add_two
                return add_two(value)

            def map2(self, value):
                return value + 1

        self.env.add_python_file(python_file_path)
        ds = self.env.from_collection([1, 2, 3, 4, 5])
        ds_1 = ds.map(lambda x: x * 2)
        ds.connect(ds_1).map(TestCoMapFunction()).add_sink(self.test_sink)
        self.env.execute("test co-map add python file")
        result = self.test_sink.get_results(True)
        expected = ['11', '3', '3', '4', '5', '5', '6', '7', '7', '9']
        result.sort()
        expected.sort()
        self.assertEqual(expected, result)

    def test_co_map_function_with_data_types(self):
        self.env.set_parallelism(1)
        ds1 = self.env.from_collection([(1, 1), (2, 2), (3, 3)],
                                       type_info=Types.ROW([Types.INT(), Types.INT()]))
        ds2 = self.env.from_collection([("a", "a"), ("b", "b"), ("c", "c")],
                                       type_info=Types.ROW([Types.STRING(), Types.STRING()]))
        ds1.connect(ds2).map(MyCoMapFunction(), output_type=Types.STRING()).add_sink(self.test_sink)
        self.env.execute('co_map_function_test')
        results = self.test_sink.get_results(False)
        expected = ['2', '3', '4', 'a', 'b', 'c']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_key_by_on_connect_stream(self):
        ds1 = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                       type_info=Types.ROW([Types.STRING(), Types.INT()])) \
            .key_by(MyKeySelector(), key_type_info=Types.INT())
        ds2 = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                       type_info=Types.ROW([Types.STRING(), Types.INT()]))

        class AssertKeyCoMapFunction(CoMapFunction):
            def __init__(self):
                self.pre1 = None
                self.pre2 = None

            def map1(self, value):
                if value[0] == 'b':
                    assert self.pre1 == 'a'
                if value[0] == 'd':
                    assert self.pre1 == 'c'
                self.pre1 = value[0]
                return value

            def map2(self, value):
                if value[0] == 'b':
                    assert self.pre2 == 'a'
                if value[0] == 'd':
                    assert self.pre2 == 'c'
                self.pre2 = value[0]
                return value

        ds1.connect(ds2)\
            .key_by(MyKeySelector(), MyKeySelector(), key_type_info=Types.INT())\
            .map(AssertKeyCoMapFunction())\
            .add_sink(self.test_sink)

        self.env.execute('key_by_test')
        results = self.test_sink.get_results(True)
        expected = ["Row(f0='e', f1=2)", "Row(f0='a', f1=0)", "Row(f0='b', f1=0)",
                    "Row(f0='c', f1=1)", "Row(f0='d', f1=1)", "Row(f0='e', f1=2)",
                    "Row(f0='a', f1=0)", "Row(f0='b', f1=0)", "Row(f0='c', f1=1)",
                    "Row(f0='d', f1=1)"]
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_map_function_with_data_types_and_function_object(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        ds.map(MyMapFunction(), output_type=Types.ROW([Types.STRING(), Types.INT(), Types.INT()]))\
            .add_sink(self.test_sink)
        self.env.execute('map_function_test')
        results = self.test_sink.get_results(False)
        expected = ['+I[ab, 2, 1]', '+I[bdc, 3, 2]', '+I[cfgs, 4, 3]', '+I[deeefg, 6, 4]']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_flat_map_function(self):
        ds = self.env.from_collection([('a', 0), ('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.flat_map(MyFlatMapFunction(), result_type=Types.ROW([Types.STRING(), Types.INT()]))\
            .add_sink(self.test_sink)

        self.env.execute('flat_map_test')
        results = self.test_sink.get_results(False)
        expected = ['+I[a, 0]', '+I[bdc, 2]', '+I[deeefg, 4]']
        results.sort()
        expected.sort()

        self.assertEqual(expected, results)

    def test_flat_map_function_with_function_object(self):
        ds = self.env.from_collection([('a', 0), ('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        def flat_map(value):
            if value[1] % 2 == 0:
                yield value

        ds.flat_map(flat_map, result_type=Types.ROW([Types.STRING(), Types.INT()]))\
            .add_sink(self.test_sink)
        self.env.execute('flat_map_test')
        results = self.test_sink.get_results(False)
        expected = ['+I[a, 0]', '+I[bdc, 2]', '+I[deeefg, 4]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_co_flat_map_function_without_data_types(self):
        self.env.set_parallelism(1)
        ds1 = self.env.from_collection([(1, 1), (2, 2), (3, 3)],
                                       type_info=Types.ROW([Types.INT(), Types.INT()]))
        ds2 = self.env.from_collection([("a", "a"), ("b", "b"), ("c", "c")],
                                       type_info=Types.ROW([Types.STRING(), Types.STRING()]))
        ds1.connect(ds2).flat_map(MyCoFlatMapFunction()).add_sink(self.test_sink)
        self.env.execute('co_flat_map_function_test')
        results = self.test_sink.get_results(True)
        expected = ['2', '2', '3', '3', '4', '4', 'b']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_co_flat_map_function_with_data_types(self):
        self.env.set_parallelism(1)
        ds1 = self.env.from_collection([(1, 1), (2, 2), (3, 3)],
                                       type_info=Types.ROW([Types.INT(), Types.INT()]))
        ds2 = self.env.from_collection([("a", "a"), ("b", "b"), ("c", "c")],
                                       type_info=Types.ROW([Types.STRING(), Types.STRING()]))
        ds1.connect(ds2).flat_map(MyCoFlatMapFunction(), output_type=Types.STRING())\
            .add_sink(self.test_sink)
        self.env.execute('co_flat_map_function_test')
        results = self.test_sink.get_results(False)
        expected = ['2', '2', '3', '3', '4', '4', 'b']
        expected.sort()
        results.sort()
        self.assertEqual(expected, results)

    def test_filter_without_data_types(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        ds.filter(MyFilterFunction()).add_sink(self.test_sink)
        self.env.execute("test filter")
        results = self.test_sink.get_results(True)
        expected = ["(2, 'Hello', 'Hi')"]
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_filter_with_data_types(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')],
                                      type_info=Types.ROW(
                                          [Types.INT(), Types.STRING(), Types.STRING()])
                                      )
        ds.filter(lambda x: x[0] % 2 == 0).add_sink(self.test_sink)
        self.env.execute("test filter")
        results = self.test_sink.get_results(False)
        expected = ['+I[2, Hello, Hi]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_add_sink(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.add_sink(self.test_sink)
        self.env.execute("test_add_sink")
        results = self.test_sink.get_results(False)
        expected = ['+I[deeefg, 4]', '+I[bdc, 2]', '+I[ab, 1]', '+I[cfgs, 3]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_execute_and_collect(self):
        test_data = ['pyflink', 'datastream', 'execute', 'collect']
        ds = self.env.from_collection(test_data)

        expected = test_data[:3]
        actual = []
        for result in ds.execute_and_collect(limit=3):
            actual.append(result)
        self.assertEqual(expected, actual)

        expected = test_data
        ds = self.env.from_collection(collection=test_data, type_info=Types.STRING())
        with ds.execute_and_collect() as results:
            actual = []
            for result in results:
                actual.append(result)
            self.assertEqual(expected, actual)

        test_data = [(1, None, 1, True, 32767, -2147483648, 1.23, 1.98932,
                      bytearray(b'flink'), 'pyflink',
                      datetime.date(2014, 9, 13),
                      datetime.time(hour=12, minute=0, second=0, microsecond=123000),
                      datetime.datetime(2018, 3, 11, 3, 0, 0, 123000),
                      [1, 2, 3],
                      [['pyflink', 'datastream'], ['execute', 'collect']],
                      decimal.Decimal('1000000000000000000.05'),
                      decimal.Decimal('1000000000000000000.0599999999999'
                                      '9999899999999999')),
                     (2, None, 2, True, 23878, 652516352, 9.87, 2.98936,
                      bytearray(b'flink'), 'pyflink',
                      datetime.date(2015, 10, 14),
                      datetime.time(hour=11, minute=2, second=2, microsecond=234500),
                      datetime.datetime(2020, 4, 15, 8, 2, 6, 235000),
                      [2, 4, 6],
                      [['pyflink', 'datastream'], ['execute', 'collect']],
                      decimal.Decimal('2000000000000000000.74'),
                      decimal.Decimal('2000000000000000000.061111111111111'
                                      '11111111111111'))]
        expected = test_data
        ds = self.env.from_collection(test_data)
        with ds.execute_and_collect() as results:
            actual = []
            for result in results:
                actual.append(result)
            self.assertEqual(expected, actual)

    def test_key_by_map(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        keyed_stream = ds.key_by(MyKeySelector(), key_type_info=Types.INT())

        with self.assertRaises(Exception):
            keyed_stream.name("keyed stream")

        class AssertKeyMapFunction(MapFunction):
            def __init__(self):
                self.pre = None
                self.state = None

            def open(self, runtime_context: RuntimeContext):
                self.state = runtime_context.get_state(
                    ValueStateDescriptor("test_state", Types.INT()))

            def map(self, value):
                state_value = self.state.value()
                if state_value is None:
                    state_value = 1
                else:
                    state_value += 1
                if value[0] == 'b':
                    assert self.pre == 'a'
                    assert state_value == 2
                if value[0] == 'd':
                    assert self.pre == 'c'
                    assert state_value == 2
                if value[0] == 'e':
                    assert state_value == 1
                self.pre = value[0]
                self.state.update(state_value)
                return value

        keyed_stream.map(AssertKeyMapFunction()).add_sink(self.test_sink)
        self.env.execute('key_by_test')
        results = self.test_sink.get_results(True)
        expected = ["Row(f0='e', f1=2)", "Row(f0='a', f1=0)", "Row(f0='b', f1=0)",
                    "Row(f0='c', f1=1)", "Row(f0='d', f1=1)"]
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_key_by_flat_map(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        keyed_stream = ds.key_by(MyKeySelector(), key_type_info=Types.INT())

        with self.assertRaises(Exception):
            keyed_stream.name("keyed stream")

        class AssertKeyMapFunction(FlatMapFunction):
            def __init__(self):
                self.pre = None
                self.state = None

            def open(self, runtime_context: RuntimeContext):
                self.state = runtime_context.get_state(
                    ValueStateDescriptor("test_state", Types.INT()))

            def flat_map(self, value):
                state_value = self.state.value()
                if state_value is None:
                    state_value = 1
                else:
                    state_value += 1
                if value[0] == 'b':
                    assert self.pre == 'a'
                    assert state_value == 2
                if value[0] == 'd':
                    assert self.pre == 'c'
                    assert state_value == 2
                if value[0] == 'e':
                    assert state_value == 1
                self.pre = value[0]
                self.state.update(state_value)
                yield value

        keyed_stream.flat_map(AssertKeyMapFunction()).add_sink(self.test_sink)
        self.env.execute('key_by_test')
        results = self.test_sink.get_results(True)
        expected = ["Row(f0='e', f1=2)", "Row(f0='a', f1=0)", "Row(f0='b', f1=0)",
                    "Row(f0='c', f1=1)", "Row(f0='d', f1=1)"]
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_key_by_filter(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        keyed_stream = ds.key_by(MyKeySelector())

        with self.assertRaises(Exception):
            keyed_stream.name("keyed stream")

        class AssertKeyFilterFunction(FilterFunction):
            def __init__(self):
                self.pre = None
                self.state = None

            def open(self, runtime_context: RuntimeContext):
                self.state = runtime_context.get_state(
                    ValueStateDescriptor("test_state", Types.INT()))

            def filter(self, value):
                state_value = self.state.value()
                if state_value is None:
                    state_value = 1
                else:
                    state_value += 1
                if value[0] == 'b':
                    assert self.pre == 'a'
                    assert state_value == 2
                    return False
                if value[0] == 'd':
                    assert self.pre == 'c'
                    assert state_value == 2
                    return False
                if value[0] == 'e':
                    assert state_value == 1
                self.pre = value[0]
                self.state.update(state_value)
                return True

        keyed_stream.filter(AssertKeyFilterFunction()).add_sink(self.test_sink)
        self.env.execute('key_by_test')
        results = self.test_sink.get_results(False)
        expected = ['+I[a, 0]', '+I[c, 1]', '+I[e, 2]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_reduce_with_state(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 1)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        keyed_stream = ds.key_by(MyKeySelector(), key_type_info=Types.INT())

        with self.assertRaises(Exception):
            keyed_stream.name("keyed stream")

        class AssertKeyReduceFunction(ReduceFunction):

            def __init__(self):
                self.state = None

            def open(self, runtime_context: RuntimeContext):
                self.state = runtime_context.get_state(
                    ValueStateDescriptor("test_state", Types.INT()))

            def reduce(self, value1, value2):
                state_value = self.state.value()
                if state_value is None:
                    state_value = 2
                else:
                    state_value += 1
                result_value = Row(value1[0] + value2[0], value1[1])
                if result_value[0] == 'ab':
                    assert state_value == 2
                if result_value[0] == 'cde':
                    assert state_value == 3
                self.state.update(state_value)
                return result_value

        keyed_stream.reduce(AssertKeyReduceFunction()).add_sink(self.test_sink)
        self.env.execute('key_by_test')
        results = self.test_sink.get_results(False)
        expected = ['+I[a, 0]', '+I[ab, 0]', '+I[c, 1]', '+I[cd, 1]', '+I[cde, 1]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_multi_key_by(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.key_by(MyKeySelector(), key_type_info=Types.INT()).key_by(lambda x: x[0])\
            .add_sink(self.test_sink)

        self.env.execute("test multi key by")
        results = self.test_sink.get_results(False)
        expected = ['+I[d, 1]', '+I[c, 1]', '+I[a, 0]', '+I[b, 0]', '+I[e, 2]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_print_without_align_output(self):
        # No need to align output typeinfo since we have specified the type info of the DataStream.
        self.env.set_parallelism(1)
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))
        ds.print()
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual("Sink: Print to Std. Out", plan['nodes'][1]['type'])

    def test_print_with_align_output(self):
        # need to align output type before print, therefore the plan will contain three nodes
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)])
        ds.print()
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(3, len(plan['nodes']))
        self.assertEqual("Sink: Print to Std. Out", plan['nodes'][2]['type'])

    def test_primitive_array_type_info(self):
        ds = self.env.from_collection([(1, [1.1, 1.2, 1.30]), (2, [2.1, 2.2, 2.3]),
                                      (3, [3.1, 3.2, 3.3])],
                                      type_info=Types.ROW([Types.INT(),
                                                           Types.PRIMITIVE_ARRAY(Types.FLOAT())]))

        ds.map(lambda x: x, output_type=Types.ROW([Types.INT(),
                                                   Types.PRIMITIVE_ARRAY(Types.FLOAT())]))\
            .add_sink(self.test_sink)
        self.env.execute("test primitive array type info")
        results = self.test_sink.get_results()
        expected = ['+I[1, [1.1, 1.2, 1.3]]', '+I[2, [2.1, 2.2, 2.3]]', '+I[3, [3.1, 3.2, 3.3]]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_basic_array_type_info(self):
        ds = self.env.from_collection([(1, [1.1, None, 1.30], [None, 'hi', 'flink']),
                                       (2, [None, 2.2, 2.3], ['hello', None, 'flink']),
                                      (3, [3.1, 3.2, None], ['hello', 'hi', None])],
                                      type_info=Types.ROW([Types.INT(),
                                                           Types.BASIC_ARRAY(Types.FLOAT()),
                                                           Types.BASIC_ARRAY(Types.STRING())]))

        ds.map(lambda x: x, output_type=Types.ROW([Types.INT(),
                                                   Types.BASIC_ARRAY(Types.FLOAT()),
                                                   Types.BASIC_ARRAY(Types.STRING())]))\
            .add_sink(self.test_sink)
        self.env.execute("test basic array type info")
        results = self.test_sink.get_results()
        expected = ['+I[1, [1.1, null, 1.3], [null, hi, flink]]',
                    '+I[2, [null, 2.2, 2.3], [hello, null, flink]]',
                    '+I[3, [3.1, 3.2, null], [hello, hi, null]]']
        results.sort()
        expected.sort()
        self.assertEqual(expected, results)

    def test_sql_timestamp_type_info(self):
        ds = self.env.from_collection([(datetime.date(2021, 1, 9),
                                        datetime.time(12, 0, 0),
                                        datetime.datetime(2021, 1, 9, 12, 0, 0, 11000))],
                                      type_info=Types.ROW([Types.SQL_DATE(),
                                                           Types.SQL_TIME(),
                                                           Types.SQL_TIMESTAMP()]))

        ds.map(lambda x: x, output_type=Types.ROW([Types.SQL_DATE(),
                                                   Types.SQL_TIME(),
                                                   Types.SQL_TIMESTAMP()]))\
            .add_sink(self.test_sink)
        self.env.execute("test sql timestamp type info")
        results = self.test_sink.get_results()
        expected = ['+I[2021-01-09, 12:00:00, 2021-01-09 12:00:00.011]']
        self.assertEqual(expected, results)

    def test_process_function(self):
        self.env.set_parallelism(1)
        self.env.get_config().set_auto_watermark_interval(2000)
        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        data_stream = self.env.from_collection([(1, '1603708211000'),
                                                (2, '1603708224000'),
                                                (3, '1603708226000'),
                                                (4, '1603708289000')],
                                               type_info=Types.ROW([Types.INT(), Types.STRING()]))

        class MyTimestampAssigner(TimestampAssigner):

            def extract_timestamp(self, value, record_timestamp) -> int:
                return int(value[1])

        class MyProcessFunction(ProcessFunction):

            def process_element(self, value, ctx):
                current_timestamp = ctx.timestamp()
                current_watermark = ctx.timer_service().current_watermark()
                yield "current timestamp: {}, current watermark: {}, current_value: {}"\
                    .format(str(current_timestamp), str(current_watermark), str(value))

            def on_timer(self, timestamp, ctx, out):
                pass

        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()\
            .with_timestamp_assigner(MyTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy)\
            .process(MyProcessFunction(), output_type=Types.STRING()).add_sink(self.test_sink)
        self.env.execute('test process function')
        result = self.test_sink.get_results()
        expected_result = ["current timestamp: 1603708211000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=1, f1='1603708211000')",
                           "current timestamp: 1603708224000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=2, f1='1603708224000')",
                           "current timestamp: 1603708226000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=3, f1='1603708226000')",
                           "current timestamp: 1603708289000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=4, f1='1603708289000')"]
        result.sort()
        expected_result.sort()
        self.assertEqual(expected_result, result)

    def test_keyed_process_function_with_state(self):
        self.env.set_parallelism(1)
        self.env.get_config().set_auto_watermark_interval(2000)
        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        data_stream = self.env.from_collection([(1, 'hi', '1603708211000'),
                                                (2, 'hello', '1603708224000'),
                                                (3, 'hi', '1603708226000'),
                                                (4, 'hello', '1603708289000'),
                                                (5, 'hi', '1603708291000'),
                                                (6, 'hello', '1603708293000')],
                                               type_info=Types.ROW([Types.INT(), Types.STRING(),
                                                                    Types.STRING()]))

        class MyTimestampAssigner(TimestampAssigner):

            def extract_timestamp(self, value, record_timestamp) -> int:
                return int(value[2])

        class MyProcessFunction(KeyedProcessFunction):

            def __init__(self):
                self.value_state = None
                self.list_state = None
                self.map_state = None

            def open(self, runtime_context: RuntimeContext):
                value_state_descriptor = ValueStateDescriptor('value_state', Types.INT())
                self.value_state = runtime_context.get_state(value_state_descriptor)
                list_state_descriptor = ListStateDescriptor('list_state', Types.INT())
                self.list_state = runtime_context.get_list_state(list_state_descriptor)
                map_state_descriptor = MapStateDescriptor('map_state', Types.INT(), Types.STRING())
                self.map_state = runtime_context.get_map_state(map_state_descriptor)

            def process_element(self, value, ctx):
                current_value = self.value_state.value()
                self.value_state.update(value[0])
                current_list = [_ for _ in self.list_state.get()]
                self.list_state.add(value[0])
                map_entries_string = []
                for k, v in self.map_state.items():
                    map_entries_string.append(str(k) + ': ' + str(v))
                map_entries_string = '{' + ', '.join(map_entries_string) + '}'
                self.map_state.put(value[0], value[1])
                current_key = ctx.get_current_key()
                yield "current key: {}, current value state: {}, current list state: {}, " \
                      "current map state: {}, current value: {}".format(str(current_key),
                                                                        str(current_value),
                                                                        str(current_list),
                                                                        map_entries_string,
                                                                        str(value))

            def on_timer(self, timestamp, ctx):
                pass

        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(MyTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[1], key_type_info=Types.STRING()) \
            .process(MyProcessFunction(), output_type=Types.STRING()) \
            .add_sink(self.test_sink)
        self.env.execute('test time stamp assigner with keyed process function')
        result = self.test_sink.get_results()
        expected_result = ["current key: hi, current value state: None, current list state: [], "
                           "current map state: {}, current value: Row(f0=1, f1='hi', "
                           "f2='1603708211000')",
                           "current key: hello, current value state: None, "
                           "current list state: [], current map state: {}, current value: Row(f0=2,"
                           " f1='hello', f2='1603708224000')",
                           "current key: hi, current value state: 1, current list state: [1], "
                           "current map state: {1: hi}, current value: Row(f0=3, f1='hi', "
                           "f2='1603708226000')",
                           "current key: hello, current value state: 2, current list state: [2], "
                           "current map state: {2: hello}, current value: Row(f0=4, f1='hello', "
                           "f2='1603708289000')",
                           "current key: hi, current value state: 3, current list state: [1, 3], "
                           "current map state: {1: hi, 3: hi}, current value: Row(f0=5, f1='hi', "
                           "f2='1603708291000')",
                           "current key: hello, current value state: 4, current list state: [2, 4],"
                           " current map state: {2: hello, 4: hello}, current value: Row(f0=6, "
                           "f1='hello', f2='1603708293000')"]
        result.sort()
        expected_result.sort()
        self.assertEqual(expected_result, result)

    def test_reducing_state(self):
        self.env.set_parallelism(2)
        data_stream = self.env.from_collection([
            (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello')],
            type_info=Types.TUPLE([Types.INT(), Types.STRING()]))

        class MyProcessFunction(KeyedProcessFunction):

            def __init__(self):
                self.reducing_state = None  # type: ReducingState

            def open(self, runtime_context: RuntimeContext):
                self.reducing_state = runtime_context.get_reducing_state(
                    ReducingStateDescriptor(
                        'reducing_state', lambda i, i2: i + i2, Types.INT()))

            def process_element(self, value, ctx):
                self.reducing_state.add(value[0])
                yield Row(self.reducing_state.get(), value[1])

        data_stream.key_by(lambda x: x[1], key_type_info=Types.STRING()) \
            .process(MyProcessFunction(), output_type=Types.TUPLE([Types.INT(), Types.STRING()])) \
            .add_sink(self.test_sink)
        self.env.execute('test_reducing_state')
        result = self.test_sink.get_results()
        expected_result = ['(1,hi)', '(2,hello)', '(4,hi)', '(6,hello)', '(9,hi)', '(12,hello)']
        result.sort()
        expected_result.sort()
        self.assertEqual(expected_result, result)

    def test_aggregating_state(self):
        self.env.set_parallelism(2)
        data_stream = self.env.from_collection([
            (1, 'hi'), (2, 'hello'), (3, 'hi'), (4, 'hello'), (5, 'hi'), (6, 'hello')],
            type_info=Types.TUPLE([Types.INT(), Types.STRING()]))

        class MyAggregateFunction(AggregateFunction):

            def create_accumulator(self):
                return 0

            def add(self, value, accumulator):
                return value + accumulator

            def get_result(self, accumulator):
                return accumulator

            def merge(self, acc_a, acc_b):
                return acc_a + acc_b

        class MyProcessFunction(KeyedProcessFunction):

            def __init__(self):
                self.aggregating_state = None  # type: AggregatingState

            def open(self, runtime_context: RuntimeContext):
                self.aggregating_state = runtime_context.get_aggregating_state(
                    AggregatingStateDescriptor(
                        'aggregating_state', MyAggregateFunction(), Types.INT()))

            def process_element(self, value, ctx):
                self.aggregating_state.add(value[0])
                yield Row(self.aggregating_state.get(), value[1])

        data_stream.key_by(lambda x: x[1], key_type_info=Types.STRING()) \
            .process(MyProcessFunction(), output_type=Types.TUPLE([Types.INT(), Types.STRING()])) \
            .add_sink(self.test_sink)
        self.env.execute('test_aggregating_state')
        result = self.test_sink.get_results()
        expected_result = ['(1,hi)', '(2,hello)', '(4,hi)', '(6,hello)', '(9,hi)', '(12,hello)']
        result.sort()
        expected_result.sort()
        self.assertEqual(expected_result, result)


class StreamingModeDataStreamTests(DataStreamTests, PyFlinkStreamingTestCase):
    def test_data_stream_name(self):
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        test_name = 'test_name'
        ds.name(test_name)
        self.assertEqual(test_name, ds.get_name())

    def test_set_parallelism(self):
        parallelism = 3
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')]).map(lambda x: x)
        ds.set_parallelism(parallelism).add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(parallelism, plan['nodes'][1]['parallelism'])

    def test_set_max_parallelism(self):
        max_parallelism = 4
        self.env.set_parallelism(8)
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')]).map(lambda x: x)
        ds.set_parallelism(max_parallelism).add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(max_parallelism, plan['nodes'][1]['parallelism'])

    def test_force_non_parallel(self):
        self.env.set_parallelism(8)
        ds = self.env.from_collection([(1, 'Hi', 'Hello'), (2, 'Hello', 'Hi')])
        ds.force_non_parallel().add_sink(self.test_sink)
        plan = eval(str(self.env.get_execution_plan()))
        self.assertEqual(1, plan['nodes'][0]['parallelism'])

    def test_union_stream(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_2 = self.env.from_collection([4, 5, 6])
        ds_3 = self.env.from_collection([7, 8, 9])

        united_stream = ds_3.union(ds_1, ds_2)

        united_stream.map(lambda x: x + 1).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        source_ids = []
        union_node_pre_ids = []
        for node in exec_plan['nodes']:
            if node['pact'] == 'Data Source':
                source_ids.append(node['id'])
            if node['pact'] == 'Operator':
                for pre in node['predecessors']:
                    union_node_pre_ids.append(pre['id'])

        source_ids.sort()
        union_node_pre_ids.sort()
        self.assertEqual(source_ids, union_node_pre_ids)

    def test_project(self):
        ds = self.env.from_collection([[1, 2, 3, 4], [5, 6, 7, 8]],
                                      type_info=Types.TUPLE(
                                          [Types.INT(), Types.INT(), Types.INT(), Types.INT()]))
        ds.project(1, 3).map(lambda x: (x[0], x[1] + 1)).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        self.assertEqual(exec_plan['nodes'][1]['type'], 'Projection')

    def test_broadcast(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.broadcast().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        broadcast_node = exec_plan['nodes'][1]
        pre_ship_strategy = broadcast_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'BROADCAST')

    def test_rebalance(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.rebalance().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        rebalance_node = exec_plan['nodes'][1]
        pre_ship_strategy = rebalance_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'REBALANCE')

    def test_rescale(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.rescale().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        rescale_node = exec_plan['nodes'][1]
        pre_ship_strategy = rescale_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'RESCALE')

    def test_shuffle(self):
        ds_1 = self.env.from_collection([1, 2, 3])
        ds_1.shuffle().map(lambda x: x + 1).set_parallelism(3).add_sink(self.test_sink)
        exec_plan = eval(self.env.get_execution_plan())
        shuffle_node = exec_plan['nodes'][1]
        pre_ship_strategy = shuffle_node['predecessors'][0]['ship_strategy']
        self.assertEqual(pre_ship_strategy, 'SHUFFLE')

    def test_partition_custom(self):
        ds = self.env.from_collection([('a', 0), ('b', 0), ('c', 1), ('d', 1), ('e', 2),
                                       ('f', 7), ('g', 7), ('h', 8), ('i', 8), ('j', 9)],
                                      type_info=Types.ROW([Types.STRING(), Types.INT()]))

        expected_num_partitions = 5

        def my_partitioner(key, num_partitions):
            assert expected_num_partitions, num_partitions
            return key % num_partitions

        partitioned_stream = ds.map(lambda x: x, output_type=Types.ROW([Types.STRING(),
                                                                        Types.INT()]))\
            .set_parallelism(4).partition_custom(my_partitioner, lambda x: x[1])

        JPartitionCustomTestMapFunction = get_gateway().jvm\
            .org.apache.flink.python.util.PartitionCustomTestMapFunction
        test_map_stream = DataStream(partitioned_stream
                                     ._j_data_stream.map(JPartitionCustomTestMapFunction()))
        test_map_stream.set_parallelism(expected_num_partitions).add_sink(self.test_sink)

        self.env.execute('test_partition_custom')

    def test_keyed_stream_partitioning(self):
        ds = self.env.from_collection([('ab', 1), ('bdc', 2), ('cfgs', 3), ('deeefg', 4)])
        keyed_stream = ds.key_by(lambda x: x[1])
        with self.assertRaises(Exception):
            keyed_stream.shuffle()

        with self.assertRaises(Exception):
            keyed_stream.rebalance()

        with self.assertRaises(Exception):
            keyed_stream.rescale()

        with self.assertRaises(Exception):
            keyed_stream.broadcast()

        with self.assertRaises(Exception):
            keyed_stream.forward()

    def test_slot_sharing_group(self):
        source_operator_name = 'collection source'
        map_operator_name = 'map_operator'
        slot_sharing_group_1 = 'slot_sharing_group_1'
        slot_sharing_group_2 = 'slot_sharing_group_2'
        ds_1 = self.env.from_collection([1, 2, 3]).name(source_operator_name)
        ds_1.slot_sharing_group(slot_sharing_group_1).map(lambda x: x + 1).set_parallelism(3)\
            .name(map_operator_name).slot_sharing_group(slot_sharing_group_2)\
            .add_sink(self.test_sink)

        j_generated_stream_graph = self.env._j_stream_execution_environment \
            .getStreamGraph("test start new_chain", True)

        j_stream_nodes = list(j_generated_stream_graph.getStreamNodes().toArray())
        for j_stream_node in j_stream_nodes:
            if j_stream_node.getOperatorName() == source_operator_name:
                self.assertEqual(j_stream_node.getSlotSharingGroup(), slot_sharing_group_1)
            elif j_stream_node.getOperatorName() == map_operator_name:
                self.assertEqual(j_stream_node.getSlotSharingGroup(), slot_sharing_group_2)

    def test_chaining_strategy(self):
        chained_operator_name_0 = "map_operator_0"
        chained_operator_name_1 = "map_operator_1"
        chained_operator_name_2 = "map_operator_2"

        ds = self.env.from_collection([1, 2, 3])
        ds.map(lambda x: x).set_parallelism(2).name(chained_operator_name_0)\
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_1)\
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_2)\
            .add_sink(self.test_sink)

        def assert_chainable(j_stream_graph, expected_upstream_chainable,
                             expected_downstream_chainable):
            j_stream_nodes = list(j_stream_graph.getStreamNodes().toArray())
            for j_stream_node in j_stream_nodes:
                if j_stream_node.getOperatorName() == chained_operator_name_1:
                    JStreamingJobGraphGenerator = get_gateway().jvm \
                        .org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator

                    j_in_stream_edge = j_stream_node.getInEdges().get(0)
                    upstream_chainable = JStreamingJobGraphGenerator.isChainable(j_in_stream_edge,
                                                                                 j_stream_graph)
                    self.assertEqual(expected_upstream_chainable, upstream_chainable)

                    j_out_stream_edge = j_stream_node.getOutEdges().get(0)
                    downstream_chainable = JStreamingJobGraphGenerator.isChainable(
                        j_out_stream_edge, j_stream_graph)
                    self.assertEqual(expected_downstream_chainable, downstream_chainable)

        # The map_operator_1 has the same parallelism with map_operator_0 and map_operator_2, and
        # ship_strategy for map_operator_0 and map_operator_1 is FORWARD, so the map_operator_1
        # can be chained with map_operator_0 and map_operator_2.
        j_generated_stream_graph = self.env._j_stream_execution_environment\
            .getStreamGraph("test start new_chain", True)
        assert_chainable(j_generated_stream_graph, True, True)

        ds = self.env.from_collection([1, 2, 3])
        # Start a new chain for map_operator_1
        ds.map(lambda x: x).set_parallelism(2).name(chained_operator_name_0) \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_1).start_new_chain() \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_2) \
            .add_sink(self.test_sink)

        j_generated_stream_graph = self.env._j_stream_execution_environment \
            .getStreamGraph("test start new_chain", True)
        # We start a new chain for map operator, therefore, it cannot be chained with upstream
        # operator, but can be chained with downstream operator.
        assert_chainable(j_generated_stream_graph, False, True)

        ds = self.env.from_collection([1, 2, 3])
        # Disable chaining for map_operator_1
        ds.map(lambda x: x).set_parallelism(2).name(chained_operator_name_0) \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_1).disable_chaining() \
            .map(lambda x: x).set_parallelism(2).name(chained_operator_name_2) \
            .add_sink(self.test_sink)

        j_generated_stream_graph = self.env._j_stream_execution_environment \
            .getStreamGraph("test start new_chain", True)
        # We disable chaining for map_operator_1, therefore, it cannot be chained with
        # upstream and downstream operators.
        assert_chainable(j_generated_stream_graph, False, False)

    def test_timestamp_assigner_and_watermark_strategy(self):
        self.env.set_parallelism(1)
        self.env.get_config().set_auto_watermark_interval(2000)
        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        data_stream = self.env.from_collection([(1, '1603708211000'),
                                                (2, '1603708224000'),
                                                (3, '1603708226000'),
                                                (4, '1603708289000')],
                                               type_info=Types.ROW([Types.INT(), Types.STRING()]))

        class MyTimestampAssigner(TimestampAssigner):

            def extract_timestamp(self, value, record_timestamp) -> int:
                return int(value[1])

        class MyProcessFunction(KeyedProcessFunction):

            def process_element(self, value, ctx):
                current_timestamp = ctx.timestamp()
                current_watermark = ctx.timer_service().current_watermark()
                current_key = ctx.get_current_key()
                yield "current key: {}, current timestamp: {}, current watermark: {}, " \
                      "current_value: {}".format(str(current_key), str(current_timestamp),
                                                 str(current_watermark), str(value))

            def on_timer(self, timestamp, ctx):
                pass

        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps()\
            .with_timestamp_assigner(MyTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy)\
            .key_by(lambda x: x[0], key_type_info=Types.INT()) \
            .process(MyProcessFunction(), output_type=Types.STRING()).add_sink(self.test_sink)
        self.env.execute('test time stamp assigner with keyed process function')
        result = self.test_sink.get_results()
        expected_result = ["current key: 1, current timestamp: 1603708211000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=1, f1='1603708211000')",
                           "current key: 2, current timestamp: 1603708224000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=2, f1='1603708224000')",
                           "current key: 3, current timestamp: 1603708226000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=3, f1='1603708226000')",
                           "current key: 4, current timestamp: 1603708289000, current watermark: "
                           "9223372036854775807, current_value: Row(f0=4, f1='1603708289000')"]
        result.sort()
        expected_result.sort()
        self.assertEqual(expected_result, result)


class BatchModeDataStreamTests(DataStreamTests, PyFlinkBatchTestCase):

    def test_timestamp_assigner_and_watermark_strategy(self):
        self.env.set_parallelism(1)
        self.env.get_config().set_auto_watermark_interval(2000)
        self.env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        data_stream = self.env.from_collection([(1, '1603708211000'),
                                                (2, '1603708224000'),
                                                (3, '1603708226000'),
                                                (4, '1603708289000')],
                                               type_info=Types.ROW([Types.INT(), Types.STRING()]))

        class MyTimestampAssigner(TimestampAssigner):

            def extract_timestamp(self, value, record_timestamp) -> int:
                return int(value[1])

        class MyProcessFunction(KeyedProcessFunction):

            def process_element(self, value, ctx):
                current_timestamp = ctx.timestamp()
                current_watermark = ctx.timer_service().current_watermark()
                current_key = ctx.get_current_key()
                yield "current key: {}, current timestamp: {}, current watermark: {}, " \
                      "current_value: {}".format(str(current_key), str(current_timestamp),
                                                 str(current_watermark), str(value))

            def on_timer(self, timestamp, ctx):
                pass

        watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
            .with_timestamp_assigner(MyTimestampAssigner())
        data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
            .key_by(lambda x: x[0], key_type_info=Types.INT()) \
            .process(MyProcessFunction(), output_type=Types.STRING()).add_sink(self.test_sink)
        self.env.execute('test time stamp assigner with keyed process function')
        result = self.test_sink.get_results()
        expected_result = ["current key: 1, current timestamp: 1603708211000, current watermark: "
                           "-9223372036854775808, current_value: Row(f0=1, f1='1603708211000')",
                           "current key: 2, current timestamp: 1603708224000, current watermark: "
                           "-9223372036854775808, current_value: Row(f0=2, f1='1603708224000')",
                           "current key: 3, current timestamp: 1603708226000, current watermark: "
                           "-9223372036854775808, current_value: Row(f0=3, f1='1603708226000')",
                           "current key: 4, current timestamp: 1603708289000, current watermark: "
                           "-9223372036854775808, current_value: Row(f0=4, f1='1603708289000')"]
        result.sort()
        expected_result.sort()
        self.assertEqual(expected_result, result)


class MyMapFunction(MapFunction):

    def map(self, value):
        result = Row(value[0], len(value[0]), value[1])
        return result


class MyFlatMapFunction(FlatMapFunction):

    def flat_map(self, value):
        if value[1] % 2 == 0:
            yield value


class MyKeySelector(KeySelector):
    def get_key(self, value):
        return value[1]


class MyFilterFunction(FilterFunction):

    def filter(self, value):
        return value[0] % 2 == 0


class MyCoMapFunction(CoMapFunction):

    def map1(self, value):
        return str(value[0] + 1)

    def map2(self, value):
        return value[0]


class MyCoFlatMapFunction(CoFlatMapFunction):

    def flat_map1(self, value):
        yield str(value[0] + 1)
        yield str(value[0] + 1)

    def flat_map2(self, value):
        if value[0] == 'b':
            yield value[0]
