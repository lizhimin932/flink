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

package org.apache.flink.runtime.blob;

import org.apache.flink.annotation.VisibleForTesting;

/**
 * BLOB key referencing permanent BLOB files.
 */
public final class PermanentBlobKey extends BlobKey {

	private static final long serialVersionUID = 5614035773213995269L;

	/**
	 * Constructs a new BLOB key.
	 */
	@VisibleForTesting
	public PermanentBlobKey() {
		super(BlobType.PERMANENT_BLOB);
	}

	/**
	 * Constructs a new BLOB key from the given byte array.
	 *
	 * @param key
	 *        the actual key data
	 */
	PermanentBlobKey(byte[] key) {
		super(BlobType.PERMANENT_BLOB, key);
	}

	/**
	 * Constructs a new BLOB key from the given byte array.
	 *
	 * @param key
	 *        the actual key data
	 * @param random
	 *        the random component of the key
	 */
	PermanentBlobKey(byte[] key, byte[] random) {
		super(BlobType.PERMANENT_BLOB, key, random);
	}
}
