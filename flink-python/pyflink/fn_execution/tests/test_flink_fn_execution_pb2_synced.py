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
import filecmp
import os

from pyflink.gen_protos import generate_proto_files
from pyflink.testing.test_case_utils import PyFlinkTestCase


class FlinkFnExecutionSyncTests(PyFlinkTestCase):
    """
    Tests whether flink_fn_exeution_pb2.py is synced with flink-fn-execution.proto.
    """

    flink_fn_execution_pb2_file_name = "flink_fn_execution_pb2.py"
    gen_protos_script = "gen_protos.py"
    flink_fn_execution_proto_file_name = "flink-fn-execution.proto"

    def test_flink_fn_execution_pb2_synced(self):
        generate_proto_files('True', self.tempdir)
        expected = os.path.join(self.tempdir, self.flink_fn_execution_pb2_file_name)
        actual = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..',
                              self.flink_fn_execution_pb2_file_name)
        self.assertTrue(filecmp.cmp(expected, actual),
                        'File %s should be re-generated by executing %s as %s has changed.'
                        % (self.flink_fn_execution_pb2_file_name,
                           self.gen_protos_script,
                           self.flink_fn_execution_proto_file_name))
