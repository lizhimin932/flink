/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common;

import com.esotericsoftware.kryo.Serializer;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.util.SerializedValue;


import java.io.IOException;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;

/**
 * A config to define the behavior of the program execution. It allows to define (among other
 * options) the following settings:
 *
 * <ul>
 *     <li>The default parallelism of the program, i.e., how many parallel tasks to use for
 *         all functions that do not define a specific value directly.</li>
 *     <li>The number of retries in the case of failed executions.</li>
 *     <li>The delay between delay between execution retries.</li>
 *     <li>The {@link ExecutionMode} of the program: Batch or Pipelined.
 *         The default execution mode is {@link ExecutionMode#PIPELINED}</li>
 *     <li>Enabling or disabling the "closure cleaner". The closure cleaner pre-processes
 *         the implementations of functions. In case they are (anonymous) inner classes,
 *         it removes unused references to the enclosing class to fix certain serialization-related
 *         problems and to reduce the size of the closure.</li>
 *     <li>The config allows to register types and serializers to increase the efficiency of
 *         handling <i>generic types</i> and <i>POJOs</i>. This is usually only needed
 *         when the functions return not only the types declared in their signature, but
 *         also subclasses of those types.</li>
 *     <li>The {@link CodeAnalysisMode} of the program: Enable hinting/optimizing or disable
 *         the "static code analyzer". The static code analyzer pre-interprets user-defined functions in order to
 *         get implementation insights for program improvements that can be printed to the log or
 *         automatically applied.</li>
 * </ul>
 */
@Public
public class ExecutionConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	// Key for storing it in the Job Configuration
	public static final String CONFIG_KEY = "runtime.config";

	/**
	 * The constant to use for the parallelism, if the system should use the number
	 *  of currently available slots.
	 */
	public static final int PARALLELISM_AUTO_MAX = Integer.MAX_VALUE;

	private static final long DEFAULT_RESTART_DELAY = 10000L;

	// --------------------------------------------------------------------------------------------

	/** Defines how data exchange happens - batch or pipelined */
	private ExecutionMode executionMode = ExecutionMode.PIPELINED;

	private boolean useClosureCleaner = true;

	private int parallelism = -1;

	/**
	 * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
	 */
	@Deprecated
	private int numberOfExecutionRetries = -1;

	private boolean forceKryo = false;

	private boolean objectReuse = false;

	private boolean autoTypeRegistrationEnabled = true;

	private boolean forceAvro = false;

	private CodeAnalysisMode codeAnalysisMode = CodeAnalysisMode.DISABLE;

	/** If set to true, progress updates are printed to System.out during execution */
	private boolean printProgressDuringExecution = true;

	private long autoWatermarkInterval = 0;

	/**
	 * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
	 */
	@Deprecated
	private long executionRetryDelay = DEFAULT_RESTART_DELAY;

	private RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration;
	
	private long taskCancellationIntervalMillis = -1;

	// ------------------------------- User code values --------------------------------------------

	private transient GlobalJobParameters globalJobParameters;

	// Serializers and types registered with Kryo and the PojoSerializer
	// we store them in linked maps/sets to ensure they are registered in order in all kryo instances.

	private LinkedHashMap<Class<?>, SerializableSerializer<?>> registeredTypesWithKryoSerializers = new LinkedHashMap<>();

	private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> registeredTypesWithKryoSerializerClasses = new LinkedHashMap<>();

	private LinkedHashMap<Class<?>, SerializableSerializer<?>> defaultKryoSerializers = new LinkedHashMap<>();

	private LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> defaultKryoSerializerClasses = new LinkedHashMap<>();

	private LinkedHashSet<Class<?>> registeredKryoTypes = new LinkedHashSet<>();

	private LinkedHashSet<Class<?>> registeredPojoTypes = new LinkedHashSet<>();

	// ----------------------- Helper values for serialized user objects ---------------------------

	private SerializedValue<GlobalJobParameters> serializedGlobalJobParameters;

	private SerializedValue<LinkedHashMap<Class<?>, SerializableSerializer<?>>> serializedRegisteredTypesWithKryoSerializers;

	private SerializedValue<LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>> serializedRegisteredTypesWithKryoSerializerClasses;

	private SerializedValue<LinkedHashMap<Class<?>, SerializableSerializer<?>>> serializedDefaultKryoSerializers;

	private SerializedValue<LinkedHashMap<Class<?>, Class<? extends Serializer<?>>>> serializedDefaultKryoSerializerClasses;

	private SerializedValue<LinkedHashSet<Class<?>>> serializedRegisteredKryoTypes;

	private SerializedValue<LinkedHashSet<Class<?>>> serializedRegisteredPojoTypes;

	// --------------------------------------------------------------------------------------------

	/**
	 * Enables the ClosureCleaner. This analyzes user code functions and sets fields to null
	 * that are not used. This will in most cases make closures or anonymous inner classes
	 * serializable that where not serializable due to some Scala or Java implementation artifact.
	 * User code must be serializable because it needs to be sent to worker nodes.
	 */
	public ExecutionConfig enableClosureCleaner() {
		useClosureCleaner = true;
		return this;
	}

	/**
	 * Disables the ClosureCleaner.
	 *
	 * @see #enableClosureCleaner()
	 */
	public ExecutionConfig disableClosureCleaner() {
		useClosureCleaner = false;
		return this;
	}

	/**
	 * Returns whether the ClosureCleaner is enabled.
	 *
	 * @see #enableClosureCleaner()
	 */
	public boolean isClosureCleanerEnabled() {
		return useClosureCleaner;
	}

	/**
	 * Sets the interval of the automatic watermark emission. Watermaks are used throughout
	 * the streaming system to keep track of the progress of time. They are used, for example,
	 * for time based windowing.
	 *
	 * @param interval The interval between watermarks in milliseconds.
	 */
	@PublicEvolving
	public ExecutionConfig setAutoWatermarkInterval(long interval) {
		this.autoWatermarkInterval = interval;
		return this;
	}

	/**
	 * Returns the interval of the automatic watermark emission.
	 *
	 * @see #setAutoWatermarkInterval(long)
	 */
	@PublicEvolving
	public long getAutoWatermarkInterval()  {
		return this.autoWatermarkInterval;
	}

	/**
	 * Gets the parallelism with which operation are executed by default. Operations can
	 * individually override this value to use a specific parallelism.
	 *
	 * Other operations may need to run with a different parallelism - for example calling
	 * a reduce operation over the entire data set will involve an operation that runs
	 * with a parallelism of one (the final reduce to the single result value).
	 *
	 * @return The parallelism used by operations, unless they override that value. This method
	 *         returns {@code -1}, if the environment's default parallelism should be used.
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as join, map, reduce) to run with
	 * x parallel instances.
	 * <p>
	 * This method overrides the default parallelism for this environment.
	 * The local execution environment uses by default a value equal to the number of hardware
	 * contexts (CPU cores / threads). When executing the program via the command line client
	 * from a JAR file, the default parallelism is the one configured for that setup.
	 *
	 * @param parallelism The parallelism to use
	 */
	public ExecutionConfig setParallelism(int parallelism) {
		if (parallelism < 1 && parallelism != -1) {
			throw new IllegalArgumentException(
					"Parallelism must be at least one, or -1 (use system default).");
		}
		this.parallelism = parallelism;
		return this;
	}

	/**
	 * Gets the interval (in milliseconds) between consecutive attempts to cancel a running task.
	 */
	public long getTaskCancellationInterval() {
		return this.taskCancellationIntervalMillis;
	}

	/**
	 * Sets the configuration parameter specifying the interval (in milliseconds)
	 * between consecutive attempts to cancel a running task.
	 * @param interval the interval (in milliseconds).
	 */
	public ExecutionConfig setTaskCancellationInterval(long interval) {
		this.taskCancellationIntervalMillis = interval;
		return this;
	}

	/**
	 * Sets the restart strategy to be used for recovery.
	 *
	 * <pre>{@code
	 * ExecutionConfig config = env.getConfig();
	 *
	 * config.setRestartStrategy(RestartStrategies.fixedDelayRestart(
	 * 	10,  // number of retries
	 * 	1000 // delay between retries));
	 * }</pre>
	 *
	 * @param restartStrategyConfiguration Configuration defining the restart strategy to use
	 */
	@PublicEvolving
	public void setRestartStrategy(RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration) {
		this.restartStrategyConfiguration = restartStrategyConfiguration;
	}

	/**
	 * Returns the restart strategy which has been set for the current job.
	 *
	 * @return The specified restart configuration
	 */
	@PublicEvolving
	public RestartStrategies.RestartStrategyConfiguration getRestartStrategy() {
		if (restartStrategyConfiguration == null) {
			// support the old API calls by creating a restart strategy from them
			if (getNumberOfExecutionRetries() > 0 && getExecutionRetryDelay() >= 0) {
				return RestartStrategies.fixedDelayRestart(getNumberOfExecutionRetries(), getExecutionRetryDelay());
			} else if (getNumberOfExecutionRetries() == 0) {
				return RestartStrategies.noRestart();
			} else {
				return null;
			}
		} else {
			return restartStrategyConfiguration;
		}
	}

	/**
	 * Gets the number of times the system will try to re-execute failed tasks. A value
	 * of {@code -1} indicates that the system default value (as defined in the configuration)
	 * should be used.
	 *
	 * @return The number of times the system will try to re-execute failed tasks.
	 *
	 * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
	 */
	@Deprecated
	public int getNumberOfExecutionRetries() {
		return numberOfExecutionRetries;
	}

	/**
	 * Returns the delay between execution retries.
	 *
	 * @return The delay between successive execution retries in milliseconds.
	 *
	 * @deprecated Should no longer be used because it is subsumed by RestartStrategyConfiguration
	 */
	@Deprecated
	public long getExecutionRetryDelay() {
		return executionRetryDelay;
	}

	/**
	 * Sets the number of times that failed tasks are re-executed. A value of zero
	 * effectively disables fault tolerance. A value of {@code -1} indicates that the system
	 * default value (as defined in the configuration) should be used.
	 *
	 * @param numberOfExecutionRetries The number of times the system will try to re-execute failed tasks.
	 *
	 * @return The current execution configuration
	 *
	 * @deprecated This method will be replaced by {@link #setRestartStrategy}. The
	 * {@link RestartStrategies.FixedDelayRestartStrategyConfiguration} contains the number of
	 * execution retries.
	 */
	@Deprecated
	public ExecutionConfig setNumberOfExecutionRetries(int numberOfExecutionRetries) {
		if (numberOfExecutionRetries < -1) {
			throw new IllegalArgumentException(
				"The number of execution retries must be non-negative, or -1 (use system default)");
		}
		this.numberOfExecutionRetries = numberOfExecutionRetries;
		return this;
	}

	/**
	 * Sets the delay between executions.
	 *
	 * @param executionRetryDelay The number of milliseconds the system will wait to retry.
	 *
	 * @return The current execution configuration
	 *
	 * @deprecated This method will be replaced by {@link #setRestartStrategy}. The
	 * {@link RestartStrategies.FixedDelayRestartStrategyConfiguration} contains the delay between
	 * successive execution attempts.
	 */
	@Deprecated
	public ExecutionConfig setExecutionRetryDelay(long executionRetryDelay) {
		if (executionRetryDelay < 0 ) {
			throw new IllegalArgumentException(
				"The delay between retries must be non-negative.");
		}
		this.executionRetryDelay = executionRetryDelay;
		return this;
	}

	/**
	 * Sets the execution mode to execute the program. The execution mode defines whether
	 * data exchanges are performed in a batch or on a pipelined manner.
	 *
	 * The default execution mode is {@link ExecutionMode#PIPELINED}.
	 *
	 * @param executionMode The execution mode to use.
	 */
	public void setExecutionMode(ExecutionMode executionMode) {
		this.executionMode = executionMode;
	}

	/**
	 * Gets the execution mode used to execute the program. The execution mode defines whether
	 * data exchanges are performed in a batch or on a pipelined manner.
	 *
	 * The default execution mode is {@link ExecutionMode#PIPELINED}.
	 *
	 * @return The execution mode for the program.
	 */
	public ExecutionMode getExecutionMode() {
		return executionMode;
	}

	/**
	 * Force TypeExtractor to use Kryo serializer for POJOS even though we could analyze as POJO.
	 * In some cases this might be preferable. For example, when using interfaces
	 * with subclasses that cannot be analyzed as POJO.
	 */
	public void enableForceKryo() {
		forceKryo = true;
	}

	/**
	 * Disable use of Kryo serializer for all POJOs.
	 */
	public void disableForceKryo() {
		forceKryo = false;
	}

	public boolean isForceKryoEnabled() {
		return forceKryo;
	}

	/**
	 * Force Flink to use the AvroSerializer for POJOs.
	 */
	public void enableForceAvro() {
		forceAvro = true;
	}

	public void disableForceAvro() {
		forceAvro = false;
	}

	public boolean isForceAvroEnabled() {
		return forceAvro;
	}

	/**
	 * Enables reusing objects that Flink internally uses for deserialization and passing
	 * data to user-code functions. Keep in mind that this can lead to bugs when the
	 * user-code function of an operation is not aware of this behaviour.
	 */
	public ExecutionConfig enableObjectReuse() {
		objectReuse = true;
		return this;
	}

	/**
	 * Disables reusing objects that Flink internally uses for deserialization and passing
	 * data to user-code functions. @see #enableObjectReuse()
	 */
	public ExecutionConfig disableObjectReuse() {
		objectReuse = false;
		return this;
	}

	/**
	 * Returns whether object reuse has been enabled or disabled. @see #enableObjectReuse()
	 */
	public boolean isObjectReuseEnabled() {
		return objectReuse;
	}
	
	/**
	 * Sets the {@link CodeAnalysisMode} of the program. Specifies to which extent user-defined
	 * functions are analyzed in order to give the Flink optimizer an insight of UDF internals
	 * and inform the user about common implementation mistakes. The static code analyzer pre-interprets
	 * user-defined functions in order to get implementation insights for program improvements
	 * that can be printed to the log, automatically applied, or disabled.
	 * 
	 * @param codeAnalysisMode see {@link CodeAnalysisMode}
	 */
	@PublicEvolving
	public void setCodeAnalysisMode(CodeAnalysisMode codeAnalysisMode) {
		this.codeAnalysisMode = codeAnalysisMode;
	}
	
	/**
	 * Returns the {@link CodeAnalysisMode} of the program.
	 */
	@PublicEvolving
	public CodeAnalysisMode getCodeAnalysisMode() {
		return codeAnalysisMode;
	}

	/**
	 * Enables the printing of progress update messages to {@code System.out}
	 * 
	 * @return The ExecutionConfig object, to allow for function chaining.
	 */
	public ExecutionConfig enableSysoutLogging() {
		this.printProgressDuringExecution = true;
		return this;
	}

	/**
	 * Disables the printing of progress update messages to {@code System.out}
	 *
	 * @return The ExecutionConfig object, to allow for function chaining.
	 */
	public ExecutionConfig disableSysoutLogging() {
		this.printProgressDuringExecution = false;
		return this;
	}

	/**
	 * Gets whether progress update messages should be printed to {@code System.out}
	 * 
	 * @return True, if progress update messages should be printed, false otherwise.
	 */
	public boolean isSysoutLoggingEnabled() {
		return this.printProgressDuringExecution;
	}

	public GlobalJobParameters getGlobalJobParameters() {
		return globalJobParameters;
	}

	/**
	 * Register a custom, serializable user configuration object.
	 * @param globalJobParameters Custom user configuration object
	 */
	public void setGlobalJobParameters(GlobalJobParameters globalJobParameters) {
		this.globalJobParameters = globalJobParameters;
	}

	// --------------------------------------------------------------------------------------------
	//  Registry for types and serializers
	// --------------------------------------------------------------------------------------------

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public <T extends Serializer<?> & Serializable>void addDefaultKryoSerializer(Class<?> type, T serializer) {
		if (type == null || serializer == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}

		defaultKryoSerializers.put(type, new SerializableSerializer<>(serializer));
	}

	/**
	 * Adds a new Kryo default serializer to the Runtime.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializerClass The class of the serializer to use.
	 */
	public void addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		if (type == null || serializerClass == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		defaultKryoSerializerClasses.put(type, serializerClass);
	}

	/**
	 * Registers the given type with a Kryo Serializer.
	 *
	 * Note that the serializer instance must be serializable (as defined by java.io.Serializable),
	 * because it may be distributed to the worker nodes by java serialization.
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializer The serializer to use.
	 */
	public <T extends Serializer<?> & Serializable>void registerTypeWithKryoSerializer(Class<?> type, T serializer) {
		if (type == null || serializer == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}

		registeredTypesWithKryoSerializers.put(type, new SerializableSerializer<>(serializer));
	}

	/**
	 * Registers the given Serializer via its class as a serializer for the given type at the KryoSerializer
	 *
	 * @param type The class of the types serialized with the given serializer.
	 * @param serializerClass The class of the serializer to use.
	 */
	public void registerTypeWithKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass) {
		if (type == null || serializerClass == null) {
			throw new NullPointerException("Cannot register null class or serializer.");
		}
		registeredTypesWithKryoSerializerClasses.put(type, serializerClass);
	}

	/**
	 * Registers the given type with the serialization stack. If the type is eventually
	 * serialized as a POJO, then the type is registered with the POJO serializer. If the
	 * type ends up being serialized with Kryo, then it will be registered at Kryo to make
	 * sure that only tags are written.
	 *
	 * @param type The class of the type to register.
	 */
	public void registerPojoType(Class<?> type) {
		if (type == null) {
			throw new NullPointerException("Cannot register null type class.");
		}
		if(!registeredPojoTypes.contains(type)) {
			registeredPojoTypes.add(type);
		}
	}

	/**
	 * Registers the given type with the serialization stack. If the type is eventually
	 * serialized as a POJO, then the type is registered with the POJO serializer. If the
	 * type ends up being serialized with Kryo, then it will be registered at Kryo to make
	 * sure that only tags are written.
	 *
	 * @param type The class of the type to register.
	 */
	public void registerKryoType(Class<?> type) {
		if (type == null) {
			throw new NullPointerException("Cannot register null type class.");
		}
		registeredKryoTypes.add(type);
	}

	/**
	 * Returns the registered types with Kryo Serializers.
	 */
	public LinkedHashMap<Class<?>, SerializableSerializer<?>> getRegisteredTypesWithKryoSerializers() {
		return registeredTypesWithKryoSerializers;
	}

	/**
	 * Returns the registered types with their Kryo Serializer classes.
	 */
	public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> getRegisteredTypesWithKryoSerializerClasses() {
		return registeredTypesWithKryoSerializerClasses;
	}


	/**
	 * Returns the registered default Kryo Serializers.
	 */
	public LinkedHashMap<Class<?>, SerializableSerializer<?>> getDefaultKryoSerializers() {
		return defaultKryoSerializers;
	}

	/**
	 * Returns the registered default Kryo Serializer classes.
	 */
	public LinkedHashMap<Class<?>, Class<? extends Serializer<?>>> getDefaultKryoSerializerClasses() {
		return defaultKryoSerializerClasses;
	}

	/**
	 * Returns the registered Kryo types.
	 */
	public LinkedHashSet<Class<?>> getRegisteredKryoTypes() {
		if (isForceKryoEnabled()) {
			// if we force kryo, we must also return all the types that
			// were previously only registered as POJO
			LinkedHashSet<Class<?>> result = new LinkedHashSet<>();
			result.addAll(registeredKryoTypes);
			for(Class<?> t : registeredPojoTypes) {
				if (!result.contains(t)) {
					result.add(t);
				}
			}
			return result;
		} else {
			return registeredKryoTypes;
		}
	}

	/**
	 * Returns the registered POJO types.
	 */
	public LinkedHashSet<Class<?>> getRegisteredPojoTypes() {
		return registeredPojoTypes;
	}


	public boolean isAutoTypeRegistrationDisabled() {
		return !autoTypeRegistrationEnabled;
	}

	/**
	 * Control whether Flink is automatically registering all types in the user programs with
	 * Kryo.
	 *
	 */
	public void disableAutoTypeRegistration() {
		this.autoTypeRegistrationEnabled = false;
	}

	/**
	 * Deserializes user code objects given a user code class loader
	 *
	 * @param userCodeClassLoader User code class loader
	 * @throws IOException Thrown if an IOException occurs while loading the classes
	 * @throws ClassNotFoundException Thrown if the given class cannot be loaded
	 */
	public void deserializeUserCode(ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {
		if (serializedRegisteredKryoTypes != null) {
			registeredKryoTypes = serializedRegisteredKryoTypes.deserializeValue(userCodeClassLoader);
		} else {
			registeredKryoTypes = new LinkedHashSet<>();
		}

		if (serializedRegisteredPojoTypes != null) {
			registeredPojoTypes = serializedRegisteredPojoTypes.deserializeValue(userCodeClassLoader);
		} else {
			registeredPojoTypes = new LinkedHashSet<>();
		}

		if (serializedRegisteredTypesWithKryoSerializerClasses != null) {
			registeredTypesWithKryoSerializerClasses = serializedRegisteredTypesWithKryoSerializerClasses.deserializeValue(userCodeClassLoader);
		} else {
			registeredTypesWithKryoSerializerClasses = new LinkedHashMap<>();
		}

		if (serializedRegisteredTypesWithKryoSerializers != null) {
			registeredTypesWithKryoSerializers = serializedRegisteredTypesWithKryoSerializers.deserializeValue(userCodeClassLoader);
		} else {
			registeredTypesWithKryoSerializerClasses = new LinkedHashMap<>();
		}

		if (serializedDefaultKryoSerializers != null) {
			defaultKryoSerializers = serializedDefaultKryoSerializers.deserializeValue(userCodeClassLoader);
		} else {
			defaultKryoSerializers = new LinkedHashMap<>();

		}

		if (serializedDefaultKryoSerializerClasses != null) {
			defaultKryoSerializerClasses = serializedDefaultKryoSerializerClasses.deserializeValue(userCodeClassLoader);
		} else {
			defaultKryoSerializerClasses = new LinkedHashMap<>();
		}

		if (serializedGlobalJobParameters != null) {
			globalJobParameters = serializedGlobalJobParameters.deserializeValue(userCodeClassLoader);
		}
	}

	public void serializeUserCode() throws IOException {
		serializedRegisteredKryoTypes = new SerializedValue<>(registeredKryoTypes);
		registeredKryoTypes = null;

		serializedRegisteredPojoTypes = new SerializedValue<>(registeredPojoTypes);
		registeredPojoTypes = null;

		serializedRegisteredTypesWithKryoSerializerClasses = new SerializedValue<>(registeredTypesWithKryoSerializerClasses);
		registeredTypesWithKryoSerializerClasses = null;

		serializedRegisteredTypesWithKryoSerializers = new SerializedValue<>(registeredTypesWithKryoSerializers);
		registeredTypesWithKryoSerializers = null;

		serializedDefaultKryoSerializers = new SerializedValue<>(defaultKryoSerializers);
		defaultKryoSerializers = null;

		serializedDefaultKryoSerializerClasses = new SerializedValue<>(defaultKryoSerializerClasses);
		defaultKryoSerializerClasses = null;

		serializedGlobalJobParameters = new SerializedValue<>(globalJobParameters);
		globalJobParameters = null;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof ExecutionConfig) {
			ExecutionConfig other = (ExecutionConfig) obj;

			return other.canEqual(this) &&
				Objects.equals(executionMode, other.executionMode) &&
				useClosureCleaner == other.useClosureCleaner &&
				parallelism == other.parallelism &&
				( (restartStrategyConfiguration == null && other.restartStrategyConfiguration == null) ||
				 (null != restartStrategyConfiguration && restartStrategyConfiguration.equals(other.restartStrategyConfiguration)) ) &&
				forceKryo == other.forceKryo &&
				objectReuse == other.objectReuse &&
				autoTypeRegistrationEnabled == other.autoTypeRegistrationEnabled &&
				forceAvro == other.forceAvro &&
				Objects.equals(codeAnalysisMode, other.codeAnalysisMode) &&
				printProgressDuringExecution == other.printProgressDuringExecution &&
				Objects.equals(globalJobParameters, other.globalJobParameters) &&
				autoWatermarkInterval == other.autoWatermarkInterval &&
				registeredTypesWithKryoSerializerClasses.equals(other.registeredTypesWithKryoSerializerClasses) &&
				defaultKryoSerializerClasses.equals(other.defaultKryoSerializerClasses) &&
				registeredKryoTypes.equals(other.registeredKryoTypes) &&
				registeredPojoTypes.equals(other.registeredPojoTypes) &&
				taskCancellationIntervalMillis == other.taskCancellationIntervalMillis;

		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			executionMode,
			useClosureCleaner,
			parallelism,
			restartStrategyConfiguration,
			forceKryo,
			objectReuse,
			autoTypeRegistrationEnabled,
			forceAvro,
			codeAnalysisMode,
			printProgressDuringExecution,
			globalJobParameters,
			autoWatermarkInterval,
			registeredTypesWithKryoSerializerClasses,
			defaultKryoSerializerClasses,
			registeredKryoTypes,
			registeredPojoTypes,
			taskCancellationIntervalMillis);
	}

	public boolean canEqual(Object obj) {
		return obj instanceof ExecutionConfig;
	}


	// ------------------------------ Utilities  ----------------------------------

	public static class SerializableSerializer<T extends Serializer<?> & Serializable> implements Serializable {
		private static final long serialVersionUID = 4687893502781067189L;

		private T serializer;

		public SerializableSerializer(T serializer) {
			this.serializer = serializer;
		}

		public T getSerializer() {
			return serializer;
		}
	}

	/**
	 * Abstract class for a custom user configuration object registered at the execution config.
	 *
	 * This user config is accessible at runtime through
	 * getRuntimeContext().getExecutionConfig().GlobalJobParameters()
	 */
	public static class GlobalJobParameters implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * Convert UserConfig into a {@code Map<String, String>} representation.
		 * This can be used by the runtime, for example for presenting the user config in the web frontend.
		 *
		 * @return Key/Value representation of the UserConfig, or null.
		 */
		public Map<String, String> toMap() {
			return null;
		}
	}
}
