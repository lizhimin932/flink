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

package org.apache.flink.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Java GC Cleaner wrapper.
 *
 * <p>A clean operation can be wrapped with the Java GC Cleaner
 * which will schedule this operation before GC is run for the given owner object (not reachable in user code).
 * but only if the cleaner has not been already run explicitly by user before.
 * If the cleaner is run after GC it will not run clean operation again.
 * This way we guarantee that the clean operation will always run at some point but only once.
 *
 * <p>The wrapper looks up the underlying Java GC Cleaner class in different packages
 */
public enum JavaGcCleanerWrapper {
	;

	private static final Logger LOG = LoggerFactory.getLogger(JavaGcCleanerWrapper.class);

	private static final Collection<CleanerProvider> CLEANER_PROVIDERS =
		Arrays.asList(createLegacyCleanerProvider(), createJava9CleanerProvider());
	private static final CleanerManager CLEANER_MANAGER = findGcCleanerManager();

	private static CleanerProvider createLegacyCleanerProvider() {
		String name = "Legacy (before Java 9) cleaner";
		ReflectionUtils reflectionUtils = new ReflectionUtils(name + " provider");
		String cleanerClassName = "sun.misc.Cleaner";

		// Actual Legacy code under the hood:
		//
		// public static Runnable createCleaner(Object owner, Runnable cleanOperation) {
		//     sun.misc.Cleaner jvmCleaner = sun.misc.Cleaner.create(owner, cleanOperation);
		//     return () -> jvmCleaner.clean();
		// }
		//
		// public static boolean tryRunPendingCleaners() throws InterruptedException {
		//     sun.misc.JavaLangRefAccess javaLangRefAccess = sun.misc.SharedSecrets.getJavaLangRefAccess();
		//	   return javaLangRefAccess.tryHandlePendingReference();
		// }
		//
		return new CleanerProvider(
			name,
			new CleanerFactoryProvider(
				name,
				reflectionUtils,
				cleanerClassName,
				Optional::empty, // there is no Cleaner object, static method of its class will be called to create it
				"create", // static method of Cleaner class to create it
				cleanerClassName, // Cleaner is Cleanable in this case
				"clean"),
			new PendingCleanersRunnerProvider(
				name,
				reflectionUtils,
				"sun.misc.SharedSecrets",
				"sun.misc.JavaLangRefAccess",
				"getJavaLangRefAccess",
				"tryHandlePendingReference"));
	}

	private static CleanerProvider createJava9CleanerProvider() {
		String name = "New Java 9+ cleaner";
		ReflectionUtils reflectionUtils = new ReflectionUtils(name + " provider");
		String cleanerClassName = "java.lang.ref.Cleaner";

		// Actual Java 9+ code under the hood:
		//
		// public static Runnable createCleaner(Object owner, Runnable cleanOperation) {
		//     java.lang.ref.Cleaner jvmCleaner = java.lang.ref.Cleaner.create();
		//     java.lang.ref.Cleaner.Cleanable cleanable = jvmCleaner.register(owner, cleanOperation);
		//     return () -> cleanable.clean();
		// }
		//
		// public static boolean tryRunPendingCleaners() throws InterruptedException {
		//     jdk.internal.misc.JavaLangRefAccess javaLangRefAccess = jdk.internal.misc.SharedSecrets.getJavaLangRefAccess();
		//	   return javaLangRefAccess.waitForReferenceProcessing();
		// }
		//
		return new CleanerProvider(
			name,
			new CleanerFactoryProvider(
				name,
				reflectionUtils,
				cleanerClassName,
				() -> {
					Class<?> cleanerClass = reflectionUtils.findClass(cleanerClassName);
					Method cleanerCreateMethod = reflectionUtils.findMethod(cleanerClass, "create");
					try {
						return Optional.of(cleanerCreateMethod.invoke(null));
					} catch (IllegalAccessException | InvocationTargetException e) {
						throw new FlinkRuntimeException("Failed to create a Java 9 Cleaner", e);
					}
				},
				"register",
				"java.lang.ref.Cleaner$Cleanable",
				"clean"),
			new PendingCleanersRunnerProvider(
				name,
				reflectionUtils,
				"jdk.internal.misc.SharedSecrets",
				"jdk.internal.misc.JavaLangRefAccess",
				"getJavaLangRefAccess",
				"waitForReferenceProcessing"));
	}

	private static CleanerManager findGcCleanerManager() {
		CleanerManager foundCleanerManager = null;
		Throwable t = null;
		for (CleanerProvider cleanerProvider : CLEANER_PROVIDERS) {
			try {
				foundCleanerManager = cleanerProvider.createCleanerManager();
				break;
			} catch (Throwable e) {
				t = ExceptionUtils.firstOrSuppressed(e, t);
			}
		}

		if (foundCleanerManager == null) {
			String errorMessage = String.format("Failed to find GC Cleaner among available providers: %s", CLEANER_PROVIDERS);
			throw new Error(errorMessage, t);
		}
		return foundCleanerManager;
	}

	public static Runnable createCleaner(Object owner, Runnable cleanOperation) {
		return CLEANER_MANAGER.create(owner, cleanOperation);
	}

	public static boolean tryRunPendingCleaners() throws InterruptedException {
		return CLEANER_MANAGER.tryRunPendingCleaners();
	}

	private static class CleanerProvider {
		private final String cleanerName;
		private final CleanerFactoryProvider cleanerFactoryProvider;
		private final PendingCleanersRunnerProvider pendingCleanersRunnerProvider;

		private CleanerProvider(
				String cleanerName,
				CleanerFactoryProvider cleanerFactoryProvider,
				PendingCleanersRunnerProvider pendingCleanersRunnerProvider) {
			this.cleanerName = cleanerName;
			this.cleanerFactoryProvider = cleanerFactoryProvider;
			this.pendingCleanersRunnerProvider = pendingCleanersRunnerProvider;
		}

		private CleanerManager createCleanerManager() {
			return new CleanerManager(
				cleanerName,
				cleanerFactoryProvider.createCleanerFactory(),
				pendingCleanersRunnerProvider.createPendingCleanersRunner());
		}

		@Override
		public String toString() {
			return cleanerName + " provider";
		}
	}

	private static class CleanerManager {
		private final String cleanerName;
		private final CleanerFactory cleanerFactory;
		private final PendingCleanersRunner pendingCleanersRunner;

		private CleanerManager(
				String cleanerName,
				CleanerFactory cleanerFactory,
				PendingCleanersRunner pendingCleanersRunner) {
			this.cleanerName = cleanerName;
			this.cleanerFactory = cleanerFactory;
			this.pendingCleanersRunner = pendingCleanersRunner;
		}

		private Runnable create(Object owner, Runnable cleanOperation) {
			return cleanerFactory.create(owner, cleanOperation);
		}

		private boolean tryRunPendingCleaners() throws InterruptedException {
			return pendingCleanersRunner.tryRunPendingCleaners();
		}

		@Override
		public String toString() {
			return cleanerName + " manager";
		}
	}

	private static class CleanerFactoryProvider {
		private final String cleanerName;
		private final ReflectionUtils reflectionUtils;
		private final String cleanerClassName;
		private final Supplier<Optional<Object>> cleanerSupplier;
		private final String cleanableCreationMethodName;
		private final String cleanableClassName;
		private final String cleanMethodName;

		private CleanerFactoryProvider(
				String cleanerName,
				ReflectionUtils reflectionUtils,
				String cleanerClassName,
				Supplier<Optional<Object>> cleanerSupplier,
				String cleanableCreationMethodName, // Cleaner is a factory for Cleanable
				String cleanableClassName,
				String cleanMethodName) {
			this.cleanerName = cleanerName;
			this.reflectionUtils = reflectionUtils;
			this.cleanerClassName = cleanerClassName;
			this.cleanerSupplier = cleanerSupplier;
			this.cleanableCreationMethodName = cleanableCreationMethodName;
			this.cleanableClassName = cleanableClassName;
			this.cleanMethodName = cleanMethodName;
		}

		private CleanerFactory createCleanerFactory() {
			Class<?> cleanerClass = reflectionUtils.findClass(cleanerClassName);
			Method cleanableCreationMethod = reflectionUtils.findMethod(
				cleanerClass,
				cleanableCreationMethodName,
				Object.class,
				Runnable.class);
			Class<?> cleanableClass = reflectionUtils.findClass(cleanableClassName);
			Method cleanMethod = reflectionUtils.findMethod(cleanableClass, cleanMethodName);
			return new CleanerFactory(
				cleanerName,
				cleanerSupplier.get().orElse(null), // static method of Cleaner class will be called to create Cleanable
				cleanableCreationMethod,
				cleanMethod);
		}

		@Override
		public String toString() {
			return cleanerName + " factory provider using " + cleanerClassName;
		}
	}

	private static class CleanerFactory {
		private final String cleanerName;
		@Nullable
		private final Object cleaner;
		private final Method cleanableCreationMethod;
		private final Method cleanMethod;

		private CleanerFactory(
			String cleanerName,
			@Nullable Object cleaner,
			Method cleanableCreationMethod,
			Method cleanMethod) {
			this.cleanerName = cleanerName;
			this.cleaner = cleaner;
			this.cleanableCreationMethod = cleanableCreationMethod;
			this.cleanMethod = cleanMethod;
		}

		private Runnable create(Object owner, Runnable cleanupOperation) {
			Object cleanable;
			try {
				cleanable = cleanableCreationMethod.invoke(cleaner, owner, cleanupOperation);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throw new Error("Failed to create a " + cleanerName, e);
			}
			String ownerString = owner.toString(); // lambda should not capture the owner object
			return () -> {
				try {
					cleanMethod.invoke(cleanable);
				} catch (IllegalAccessException | InvocationTargetException e) {
					String message = String.format("FATAL UNEXPECTED - Failed to invoke a %s for %s", cleanerName, ownerString);
					LOG.error(message, e);
					throw new Error(message, e);
				}
			};
		}
	}

	private static class PendingCleanersRunnerProvider {
		private final String cleanerName;
		private final ReflectionUtils reflectionUtils;
		private final String sharedSecretsClassName;
		private final String javaLangRefAccessClassName;
		private final String getJavaLangRefAccessName;
		private final String tryHandlePendingReferenceName;

		private PendingCleanersRunnerProvider(
				String cleanerName,
				ReflectionUtils reflectionUtils,
				String sharedSecretsClassName,
				String javaLangRefAccessClassName,
				String getJavaLangRefAccessName,
				String tryHandlePendingReferenceName) {
			this.cleanerName = cleanerName;
			this.reflectionUtils = reflectionUtils;
			this.sharedSecretsClassName = sharedSecretsClassName;
			this.javaLangRefAccessClassName = javaLangRefAccessClassName;
			this.getJavaLangRefAccessName = getJavaLangRefAccessName;
			this.tryHandlePendingReferenceName = tryHandlePendingReferenceName;
		}

		private PendingCleanersRunner createPendingCleanersRunner() {
			Class<?> sharedSecretsClass = reflectionUtils.findClass(sharedSecretsClassName);
			Class<?> javaLangRefAccessClass = reflectionUtils.findClass(javaLangRefAccessClassName);
			Method getJavaLangRefAccessMethod = reflectionUtils.findMethod(sharedSecretsClass, getJavaLangRefAccessName);
			Method tryHandlePendingReferenceMethod = reflectionUtils.findMethod(
				javaLangRefAccessClass,
				tryHandlePendingReferenceName);
			return new PendingCleanersRunner(getJavaLangRefAccessMethod, tryHandlePendingReferenceMethod);
		}

		@Override
		public String toString() {
			return "Pending " + cleanerName + "s runner provider";
		}
	}

	private static class PendingCleanersRunner {
		private final Method getJavaLangRefAccessMethod;
		private final Method waitForReferenceProcessingMethod;

		private PendingCleanersRunner(Method getJavaLangRefAccessMethod, Method waitForReferenceProcessingMethod) {
			this.getJavaLangRefAccessMethod = getJavaLangRefAccessMethod;
			this.waitForReferenceProcessingMethod = waitForReferenceProcessingMethod;
		}

		private boolean tryRunPendingCleaners() throws InterruptedException {
			Object javaLangRefAccess = getJavaLangRefAccess();
			try {
				return (Boolean) waitForReferenceProcessingMethod.invoke(javaLangRefAccess);
			} catch (IllegalAccessException | InvocationTargetException e) {
				throwIfCauseIsInterruptedException(e);
				return throwInvocationError(e, javaLangRefAccess, waitForReferenceProcessingMethod);
			}
		}

		private Object getJavaLangRefAccess() {
			try {
				return getJavaLangRefAccessMethod.invoke(null);
			} catch (IllegalAccessException | InvocationTargetException e) {
				return throwInvocationError(e, null, waitForReferenceProcessingMethod);
			}
		}

		private static void throwIfCauseIsInterruptedException(Throwable t) throws InterruptedException {
			// if the original wrapped method can throw InterruptedException
			// then we may want to explicitly handle in the user code for certain implementations
			if (t.getCause() instanceof InterruptedException) {
				throw (InterruptedException) t.getCause();
			}
		}

		private static <T> T throwInvocationError(Throwable t, @Nullable Object obj, Method method) {
			String message = String.format(
				"FATAL UNEXPECTED - Failed to invoke %s%s",
				obj == null ? "" : obj.getClass().getSimpleName() + '#',
				method.getName());
			LOG.error(message, t);
			throw new Error(message, t);
		}
	}

	private static class ReflectionUtils {
		private final String logPrefix;

		private ReflectionUtils(String logPrefix) {
			this.logPrefix = logPrefix;
		}

		private Class<?> findClass(String className) {
			try {
				return Class.forName(className);
			} catch (ClassNotFoundException e) {
				throw new FlinkRuntimeException(
					String.format("%s: Failed to find %s class", logPrefix, className.split("\\.")[0]),
					e);
			}
		}

		private Method findMethod(Class<?> cl, String methodName, Class<?>... parameterTypes) {
			try {
				return cl.getMethod(methodName, parameterTypes);
			} catch (NoSuchMethodException e) {
				throw new FlinkRuntimeException(
					String.format("%s: Failed to find %s#%s method", logPrefix, cl.getSimpleName(), methodName),
					e);
			}
		}
	}
}
