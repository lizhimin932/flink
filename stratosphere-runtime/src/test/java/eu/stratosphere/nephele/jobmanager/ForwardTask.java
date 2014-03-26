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

package eu.stratosphere.nephele.jobmanager;

import eu.stratosphere.runtime.io.api.RecordReader;
import eu.stratosphere.runtime.io.api.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.types.Record;

public class ForwardTask extends AbstractTask {

	private RecordReader<Record> input = null;
	private RecordWriter<Record> output = null;

	@Override
	public void invoke() throws Exception {

		this.output.initializeSerializers();

		while (this.input.hasNext()) {

			Record s = input.next();
			this.output.emit(s);
		}

		this.output.flush();
	}

	@Override
	public void registerInputOutput() {
		this.input = new RecordReader<Record>(this, Record.class);
		this.output = new RecordWriter<Record>(this);
	}
}
