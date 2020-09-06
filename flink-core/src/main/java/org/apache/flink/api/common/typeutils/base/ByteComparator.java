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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.MemorySegment;

@Internal
public final class ByteComparator extends BasicTypeComparator<Byte> {

	private static final long serialVersionUID = 1L;

	
	public ByteComparator(boolean ascending) {
		super(ascending);
	}

	@Override
	public int compareSerialized(DataInputView firstSource, DataInputView secondSource) throws IOException {
		byte b1 = firstSource.readByte();
		byte b2 = secondSource.readByte();
		int comp = (b1 < b2 ? -1 : (b1 == b2 ? 0 : 1)); 
		return ascendingComparison ? comp : -comp; 
	}


	@Override
	public boolean supportsNormalizedKey() {
		return true;
	}

	@Override
	public int getNormalizeKeyLen() {
		return 1;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return keyBytes < 1;
	}

	@Override
	public void putNormalizedKey(Byte value, MemorySegment target, int offset, int numBytes) {
		NormalizedKeyUtil.putByteNormalizedKey(value, target, offset, numBytes);
	}

	@Override
	public ByteComparator duplicate() {
		return new ByteComparator(ascendingComparison);
	}
}
