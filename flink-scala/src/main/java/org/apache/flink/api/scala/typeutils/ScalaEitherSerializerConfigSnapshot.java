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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import scala.util.Either;

/**
 * Configuration snapshot for serializers of Scala's {@link Either} type,
 * containing configuration snapshots of the Left and Right serializers.
 */
public class ScalaEitherSerializerConfigSnapshot<E extends Either<L, R>, L, R>
		extends CompositeTypeSerializerConfigSnapshot<E> {

	private static final int VERSION = 1;

	/** This empty nullary constructor is required for deserializing the configuration. */
	public ScalaEitherSerializerConfigSnapshot() {}

	public ScalaEitherSerializerConfigSnapshot(TypeSerializer<L> leftSerializer, TypeSerializer<R> rightSerializer) {
		super(leftSerializer, rightSerializer);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected TypeSerializer<E> restoreSerializer(TypeSerializer<?>... restoredNestedSerializers) {
		return new EitherSerializer<>(
			(TypeSerializer<L>) restoredNestedSerializers[0],
			(TypeSerializer<R>) restoredNestedSerializers[1]);
	}

	@Override
	protected boolean isRecognizableSerializer(TypeSerializer<?> newSerializer) {
		return newSerializer instanceof EitherSerializer;
	}
}
