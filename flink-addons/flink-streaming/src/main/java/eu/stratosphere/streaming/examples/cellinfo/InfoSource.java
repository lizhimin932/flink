/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.examples.cellinfo;

import java.util.Random;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class InfoSource extends UserSourceInvokable {
	private static final long serialVersionUID = 1L;
	
	Random rand = new Random();
	private final static int CELL_COUNT = 10;

	private StreamRecord record = new StreamRecord(new Tuple2<Integer, Long>());

	@Override
	public void invoke() throws Exception {
		for (int i = 0; i < 50000; i++) {
			record.setInteger(0, rand.nextInt(CELL_COUNT));
			record.setLong(1, System.currentTimeMillis());

			emit(record);
		}
	}

}
