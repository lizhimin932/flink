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

package org.apache.flink.runtime.rest.messages.job;

import org.apache.flink.runtime.rest.messages.ConversionException;
import org.apache.flink.runtime.rest.messages.MessageParameter;
import org.apache.flink.runtime.rest.messages.MessageQueryParameter;

import java.util.Collections;
import java.util.List;

/**
 * Specifies the upper limit of exceptions to return for JobExceptionsHandler.
 *
 * @see org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler
 */
public class UpperLimitExceptionParameter extends MessageQueryParameter<Integer> {

    public static final String KEY = "maxExceptions";

    public UpperLimitExceptionParameter() {
        super(KEY, MessageParameter.MessageParameterRequisiteness.OPTIONAL);
    }

    @Override
    public List<Integer> convertFromString(String values) throws ConversionException {
        String[] splitValues = values.split(",");
        if (splitValues.length != 1) {
            throw new ConversionException(
                    String.format(
                            "%s may be a single integer value that specifies the upper limit of exceptions to return (%s)",
                            KEY, values));
        }
        return Collections.singletonList(convertStringToValue(splitValues[0]));
    }

    @Override
    public Integer convertStringToValue(String value) {
        return Integer.valueOf(value);
    }

    @Override
    public String convertValueToString(Integer value) {
        return value.toString();
    }

    @Override
    public String getDescription() {
        return "An integer value that specifies the upper limit of exceptions to return.";
    }
}
