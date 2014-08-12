/**
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

package org.apache.flink.runtime.deployment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.io.StringRecord;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.util.SerializableArrayList;
import org.apache.flink.util.StringUtils;

/**
 * A task deployment descriptor contains all the information necessary to deploy a task on a task manager.
 * <p>
 * This class is not thread-safe in general.
 */
public final class TaskDeploymentDescriptor implements IOReadableWritable {

	/**
	 * The ID of the job the tasks belongs to.
	 */
	private final JobID jobID;

	/**
	 * The task's execution vertex ID.
	 */
	private final ExecutionVertexID vertexID;

	/**
	 * The task's name.
	 */
	private String taskName;

	/**
	 * The task's index in the subtask group.
	 */
	private int indexInSubtaskGroup;

	/**
	 * The current number of subtasks.
	 */
	private int currentNumberOfSubtasks;

	/**
	 * The configuration of the job the task belongs to.
	 */
	private Configuration jobConfiguration;

	/**
	 * The task's configuration object.
	 */
	private Configuration taskConfiguration;

	/**
	 * The class containing the task code to be executed.
	 */
	private Class<? extends AbstractInvokable> invokableClass;

	/**
	 * The list of output gate deployment descriptors.
	 */
	private final SerializableArrayList<GateDeploymentDescriptor> outputGates;

	/**
	 * The list of input gate deployment descriptors.
	 */
	private final SerializableArrayList<GateDeploymentDescriptor> inputGates;

	/**
	 * The list of JAR files required to run this task.
	 */
	private final List<BlobKey> requiredJarFiles;

	/**
	 * Constructs a task deployment descriptor.
	 * 
	 * @param jobID
	 *        the ID of the job the tasks belongs to
	 * @param vertexID
	 *        the task's execution vertex ID
	 * @param taskName
	 *        the task's name the task's index in the subtask group
	 * @param indexInSubtaskGroup
	 *        he task's index in the subtask group
	 * @param currentNumberOfSubtasks
	 *        the current number of subtasks
	 * @param jobConfiguration
	 *        the configuration of the job the task belongs to
	 * @param taskConfiguration
	 *        the task's configuration object
	 * @param invokableClass
	 *        the class containing the task code to be executed
	 * @param outputGates
	 *        list of output gate deployment descriptors
	 * @param inputGateIDs
	 *        list of input gate deployment descriptors
	 * @param requiredJarFiles
	 *        list of JAR files required to run this task
	 */
	public TaskDeploymentDescriptor(final JobID jobID, final ExecutionVertexID vertexID, final String taskName,
			final int indexInSubtaskGroup, final int currentNumberOfSubtasks, final Configuration jobConfiguration,
			final Configuration taskConfiguration,
			final Class<? extends AbstractInvokable> invokableClass,
			final SerializableArrayList<GateDeploymentDescriptor> outputGates,
			final SerializableArrayList<GateDeploymentDescriptor> inputGates,
			final List<BlobKey> requiredJarFiles) {

		if (jobID == null) {
			throw new IllegalArgumentException("Argument jobID must not be null");
		}

		if (vertexID == null) {
			throw new IllegalArgumentException("Argument vertexID must not be null");
		}

		if (taskName == null) {
			throw new IllegalArgumentException("Argument taskName must not be null");
		}

		if (indexInSubtaskGroup < 0) {
			throw new IllegalArgumentException("Argument indexInSubtaskGroup must not be smaller than zero");
		}

		if (currentNumberOfSubtasks < indexInSubtaskGroup) {
			throw new IllegalArgumentException(
				"Argument currentNumberOfSubtasks must not be smaller than argument indexInSubtaskGroup");
		}

		if (jobConfiguration == null) {
			throw new IllegalArgumentException("Argument jobConfiguration must not be null");
		}

		if (taskConfiguration == null) {
			throw new IllegalArgumentException("Argument taskConfiguration must not be null");
		}

		if (invokableClass == null) {
			throw new IllegalArgumentException("Argument invokableClass must not be null");
		}

		if (outputGates == null) {
			throw new IllegalArgumentException("Argument outputGates must not be null");
		}

		if (inputGates == null) {
			throw new IllegalArgumentException("Argument inputGates must not be null");
		}

		if (requiredJarFiles == null) {
			throw new IllegalArgumentException("Argument requiredJarFiles must not be null");
		}

		this.jobID = jobID;
		this.vertexID = vertexID;
		this.taskName = taskName;
		this.indexInSubtaskGroup = indexInSubtaskGroup;
		this.currentNumberOfSubtasks = currentNumberOfSubtasks;
		this.jobConfiguration = jobConfiguration;
		this.taskConfiguration = taskConfiguration;
		this.invokableClass = invokableClass;
		this.outputGates = outputGates;
		this.inputGates = inputGates;
		this.requiredJarFiles = requiredJarFiles;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public TaskDeploymentDescriptor() {

		this.jobID = new JobID();
		this.vertexID = new ExecutionVertexID();
		this.taskName = null;
		this.indexInSubtaskGroup = 0;
		this.currentNumberOfSubtasks = 0;
		this.jobConfiguration = new Configuration();
		this.taskConfiguration = new Configuration();
		this.invokableClass = null;
		this.outputGates = new SerializableArrayList<GateDeploymentDescriptor>();
		this.inputGates = new SerializableArrayList<GateDeploymentDescriptor>();
		this.requiredJarFiles = new ArrayList<BlobKey>();
	}

	@Override
	public void write(final DataOutputView out) throws IOException {

		this.jobID.write(out);
		this.vertexID.write(out);
		StringRecord.writeString(out, this.taskName);
		out.writeInt(this.indexInSubtaskGroup);
		out.writeInt(this.currentNumberOfSubtasks);

		// Write out the BLOB keys of the required JAR files
		out.writeInt(this.requiredJarFiles.size());
		for (final Iterator<BlobKey> it = this.requiredJarFiles.iterator(); it.hasNext();) {
			it.next().write(out);
		}

		// Write out the name of the invokable class
		if (this.invokableClass == null) {
			throw new IOException("this.invokableClass is null");
		}

		StringRecord.writeString(out, this.invokableClass.getName());

		this.jobConfiguration.write(out);
		this.taskConfiguration.write(out);

		this.outputGates.write(out);
		this.inputGates.write(out);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void read(final DataInputView in) throws IOException {

		this.jobID.read(in);
		this.vertexID.read(in);
		this.taskName = StringRecord.readString(in);
		this.indexInSubtaskGroup = in.readInt();
		this.currentNumberOfSubtasks = in.readInt();

		// Read BLOB keys of required jar files
		final int numberOfJarFiles = in.readInt();
		for (int i = 0; i < numberOfJarFiles; ++i) {
			final BlobKey key = new BlobKey();
			key.read(in);
			this.requiredJarFiles.add(key);
		}

		// Now register data with the library manager
		LibraryCacheManager.register(this.jobID, this.requiredJarFiles);

		// Get ClassLoader from Library Manager
		final ClassLoader cl = LibraryCacheManager.getClassLoader(this.jobID);

		// Read the name of the invokable class;
		final String invokableClassName = StringRecord.readString(in);

		if (invokableClassName == null) {
			throw new IOException("invokableClassName is null");
		}

		try {
			this.invokableClass = (Class<? extends AbstractInvokable>) Class.forName(invokableClassName, true, cl);
		} catch (ClassNotFoundException cnfe) {
			throw new IOException("Class " + invokableClassName + " not found in one of the supplied jar files: "
				+ StringUtils.stringifyException(cnfe));
		}

		this.jobConfiguration = new Configuration(cl);
		this.jobConfiguration.read(in);
		this.taskConfiguration = new Configuration(cl);
		this.taskConfiguration.read(in);

		this.outputGates.read(in);
		this.inputGates.read(in);
	}

	/**
	 * Returns the ID of the job the tasks belongs to.
	 * 
	 * @return the ID of the job the tasks belongs to
	 */
	public JobID getJobID() {

		return this.jobID;
	}

	/**
	 * Returns the task's execution vertex ID.
	 * 
	 * @return the task's execution vertex ID
	 */
	public ExecutionVertexID getVertexID() {

		return this.vertexID;
	}

	/**
	 * Returns the task's name.
	 * 
	 * @return the task's name
	 */
	public String getTaskName() {

		return this.taskName;
	}

	/**
	 * Returns the task's index in the subtask group.
	 * 
	 * @return the task's index in the subtask group
	 */
	public int getIndexInSubtaskGroup() {

		return this.indexInSubtaskGroup;
	}

	/**
	 * Returns the current number of subtasks.
	 * 
	 * @return the current number of subtasks
	 */
	public int getCurrentNumberOfSubtasks() {

		return this.currentNumberOfSubtasks;
	}

	/**
	 * Returns the configuration of the job the task belongs to.
	 * 
	 * @return the configuration of the job the tasks belongs to
	 */
	public Configuration getJobConfiguration() {

		return this.jobConfiguration;
	}

	/**
	 * Returns the task's configuration object.
	 * 
	 * @return the task's configuration object
	 */
	public Configuration getTaskConfiguration() {

		return this.taskConfiguration;
	}

	/**
	 * Returns the class containing the task code to be executed.
	 * 
	 * @return the class containing the task code to be executed
	 */
	public Class<? extends AbstractInvokable> getInvokableClass() {

		return this.invokableClass;
	}

	/**
	 * Returns the number of output gate deployment descriptors contained in this task deployment descriptor.
	 * 
	 * @return the number of output gate deployment descriptors
	 */
	public int getNumberOfOutputGateDescriptors() {

		return this.outputGates.size();
	}

	/**
	 * Returns the output gate descriptor with the given index
	 * 
	 * @param index
	 *        the index if the output gate descriptor to return
	 * @return the output gate descriptor with the given index
	 */
	public GateDeploymentDescriptor getOutputGateDescriptor(final int index) {

		return this.outputGates.get(index);
	}

	/**
	 * Returns the number of output gate deployment descriptors contained in this task deployment descriptor.
	 * 
	 * @return the number of output gate deployment descriptors
	 */
	public int getNumberOfInputGateDescriptors() {

		return this.inputGates.size();
	}

	/**
	 * Returns the input gate descriptor with the given index
	 * 
	 * @param index
	 *        the index if the input gate descriptor to return
	 * @return the input gate descriptor with the given index
	 */
	public GateDeploymentDescriptor getInputGateDescriptor(final int index) {

		return this.inputGates.get(index);
	}
}