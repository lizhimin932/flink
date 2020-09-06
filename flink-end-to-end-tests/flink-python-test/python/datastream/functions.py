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

from pyflink.datastream.functions import CoMapFunction


def str_len(value):
    return value[0], len(value[0]), value[1]


def add_one(value):
    return value[0], value[1] + 1, value[1]


def m_flat_map(value):
    for i in range(value[1]):
        yield value[0], i, value[2]


class MyCoMapFunction(CoMapFunction):
    def map1(self, value):
        return str_len(value)

    def map2(self, value):
        return add_one(value)
