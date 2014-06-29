/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/
package eu.stratosphere.api.common.distributions;

import java.io.IOException;

import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.DoubleValue;


public class UniformDoubleDistribution implements DataDistribution {

	private static final long serialVersionUID = 1L;
	
	private double min, max; 

	
	public UniformDoubleDistribution() {}
	
	public UniformDoubleDistribution(double min, double max) {
		this.min = min;
		this.max = max;
	}

	@Override
	public DoubleValue[] getBucketBoundary(int bucketNum, int totalNumBuckets) {
		double bucketSize = (max - min) / totalNumBuckets;
		return new DoubleValue[] {new DoubleValue(min + (bucketNum+1) * bucketSize) };
	}

	@Override
	public int getNumberOfFields() {
		return 1;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		out.writeDouble(min);
		out.writeDouble(max);
	}

	@Override
	public void read(DataInputView in) throws IOException {
		min = in.readDouble();
		max = in.readDouble();
	}
}
