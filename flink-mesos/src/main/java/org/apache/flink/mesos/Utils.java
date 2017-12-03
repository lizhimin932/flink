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

package org.apache.flink.mesos;

import org.apache.flink.mesos.util.MesosArtifactResolver;
import org.apache.flink.runtime.clusterframework.ContainerSpecification;

import org.apache.mesos.Protos;

import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import scala.Option;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Collection of utility methods.
 */
public class Utils {
	/**
	 * Construct a Mesos environment variable.
	 */
	public static Protos.Environment.Variable variable(String name, String value) {
		return Protos.Environment.Variable.newBuilder()
			.setName(name)
			.setValue(value)
			.build();
	}

	/**
	 * Construct a Mesos URI.
	 */
	public static Protos.CommandInfo.URI uri(URL url, boolean cacheable) {
		return Protos.CommandInfo.URI.newBuilder()
			.setValue(url.toExternalForm())
			.setExtract(false)
			.setCache(cacheable)
			.build();
	}

	/**
	 * Construct a Mesos URI.
	 */
	public static Protos.CommandInfo.URI uri(MesosArtifactResolver resolver, ContainerSpecification.Artifact artifact) {
		Option<URL> url = resolver.resolve(artifact.dest);
		if (url.isEmpty()) {
			throw new IllegalArgumentException("Unresolvable artifact: " + artifact.dest);
		}

		return Protos.CommandInfo.URI.newBuilder()
			.setValue(url.get().toExternalForm())
			.setOutputFile(artifact.dest.toString())
			.setExtract(artifact.extract)
			.setCache(artifact.cachable)
			.setExecutable(artifact.executable)
			.build();
	}

	/**
	 * Construct a scalar resource value.
	 */
	public static Protos.Resource scalar(String name, String role, double value) {
		return Protos.Resource.newBuilder()
			.setName(name)
			.setType(Protos.Value.Type.SCALAR)
			.setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
			.setRole(role)
			.build();
	}

	/**
	 * Construct a range value.
	 */
	public static Protos.Value.Range range(long begin, long end) {
		return Protos.Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
	}

	/**
	 * Construct a range resource.
	 */
	public static Protos.Resource ranges(String name, String role, Protos.Value.Range... ranges) {
		return Protos.Resource.newBuilder()
			.setName(name)
			.setType(Protos.Value.Type.RANGES)
			.setRanges(Protos.Value.Ranges.newBuilder().addAllRange(Arrays.asList(ranges)).build())
			.setRole(role)
			.build();
	}

	/**
	 * Gets a stream of values from a collection of range resources.
	 */
	public static LongStream rangeValues(Collection<Protos.Resource> resources) {
		return resources.stream()
			.filter(Protos.Resource::hasRanges)
			.flatMap(r -> r.getRanges().getRangeList().stream())
			.flatMapToLong(Utils::rangeValues);
	}

	/**
	 * Gets a stream of values from a range.
	 */
	public static LongStream rangeValues(Protos.Value.Range range) {
		return LongStream.rangeClosed(range.getBegin(), range.getEnd());
	}

	/**
	 * Gets a string representation of a collection of resources.
	 */
	public static String print(Collection<Protos.Resource> resources) {
		checkNotNull(resources);
		return resources.stream().map(Utils::print).collect(Collectors.joining("; ", "[", "]"));
	}

	/**
	 * Gets a string representation of a resource.
	 */
	public static String print(Protos.Resource resource) {
		checkNotNull(resource);
		if (resource.hasScalar()) {
			return String.format("%s(%s):%.1f", resource.getName(), resource.getRole(), resource.getScalar().getValue());
		}
		if (resource.hasRanges()) {
			return String.format("%s(%s):%s", resource.getName(), resource.getRole(), print(resource.getRanges()));
		}
		return resource.toString();
	}

	/**
	 * Gets a string representation of a collection of ranges.
	 */
	public static String print(Protos.Value.Ranges ranges) {
		checkNotNull(ranges);
		return ranges.getRangeList().stream().map(Utils::print).collect(Collectors.joining(",", "[", "]"));
	}

	/**
	 * Gets a string representation of a range.
	 */
	public static String print(Protos.Value.Range range) {
		checkNotNull(range);
		return String.format("%d-%d", range.getBegin(), range.getEnd());
	}
}
