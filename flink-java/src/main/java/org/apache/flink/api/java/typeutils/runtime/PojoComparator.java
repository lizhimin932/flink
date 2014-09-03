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

package org.apache.flink.api.java.typeutils.runtime;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.types.NullKeyFieldException;
import org.apache.flink.util.InstantiationUtil;


public final class PojoComparator<T> extends TypeComparator<T> implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	// for each "field" we have a list of fields, since we might access fields
	// in nested objects
	private transient List<Field>[] keyFields;

	private final TypeComparator<Object>[] comparators;

	private final int[] normalizedKeyLengths;

	private final int numLeadingNormalizableKeys;

	private final int normalizableKeyPrefixLen;

	private final boolean invertNormKey;

	private TypeSerializer<T> serializer;

	private final Class<T> type;


	@SuppressWarnings("unchecked")
	public PojoComparator(List<Field>[] keyFields, TypeComparator<?>[] comparators, TypeSerializer<T> serializer, Class<T> type) {
		this.keyFields = keyFields;
		this.comparators = (TypeComparator<Object>[]) comparators;

		this.type = type;
		this.serializer = serializer;

		// set up auxiliary fields for normalized key support
		this.normalizedKeyLengths = new int[keyFields.length];
		int nKeys = 0;
		int nKeyLen = 0;
		boolean inverted = false;

		for (int i = 0; i < this.comparators.length; i++) {
			TypeComparator<?> k = this.comparators[i];

			// as long as the leading keys support normalized keys, we can build up the composite key
			if (k.supportsNormalizedKey()) {
				if (i == 0) {
					// the first comparator decides whether we need to invert the key direction
					inverted = k.invertNormalizedKey();
				}
				else if (k.invertNormalizedKey() != inverted) {
					// if a successor does not agree on the invertion direction, it cannot be part of the normalized key
					break;
				}

				nKeys++;
				final int len = k.getNormalizeKeyLen();
				if (len < 0) {
					throw new RuntimeException("Comparator " + k.getClass().getName() + " specifies an invalid length for the normalized key: " + len);
				}
				this.normalizedKeyLengths[i] = len;
				nKeyLen += this.normalizedKeyLengths[i];

				if (nKeyLen < 0) {
					// overflow, which means we are out of budget for normalized key space anyways
					nKeyLen = Integer.MAX_VALUE;
					break;
				}
			} else {
				break;
			}
		}
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
		this.invertNormKey = inverted;
	}

	@SuppressWarnings("unchecked")
	private PojoComparator(PojoComparator<T> toClone) {
		this.keyFields = toClone.keyFields;
		this.comparators = new TypeComparator[toClone.comparators.length];

		for (int i = 0; i < toClone.comparators.length; i++) {
			this.comparators[i] = toClone.comparators[i].duplicate();
		}

		this.normalizedKeyLengths = toClone.normalizedKeyLengths;
		this.numLeadingNormalizableKeys = toClone.numLeadingNormalizableKeys;
		this.normalizableKeyPrefixLen = toClone.normalizableKeyPrefixLen;
		this.invertNormKey = toClone.invertNormKey;

		this.type = toClone.type;

		try {
			this.serializer = (TypeSerializer<T>) InstantiationUtil.deserializeObject(
					InstantiationUtil.serializeObject(toClone.serializer), Thread.currentThread().getContextClassLoader());
		} catch (IOException e) {
			throw new RuntimeException("Cannot copy serializer", e);
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Cannot copy serializer", e);
		}


	}

	private void writeObject(ObjectOutputStream out)
			throws IOException, ClassNotFoundException {
		out.defaultWriteObject();
		out.writeInt(keyFields.length);
		for (List<Field> fieldAccessors: keyFields) {
			out.writeInt(fieldAccessors.size());
			for (Field field: fieldAccessors) {
				out.writeObject(field.getDeclaringClass());
				out.writeUTF(field.getName());
			}
		}
	}

	private void readObject(ObjectInputStream in)
			throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		int numKeyFields = in.readInt();
		keyFields = new List[numKeyFields];
		for (int i = 0; i < numKeyFields; i++) {
			int numAccessors = in.readInt();
			keyFields[i] = new ArrayList<Field>();
			for (int j = 0; j < numAccessors; j++) {
				Class<?> clazz = (Class<?>) in.readObject();
				String fieldName = in.readUTF();
				// try superclasses as well
				while (clazz != null) {
					try {
						Field field = clazz.getDeclaredField(fieldName);
						field.setAccessible(true);
						keyFields[i].add(field);
						break;
					} catch (NoSuchFieldException e) {
						clazz = clazz.getSuperclass();
					}
				}
				if (keyFields[i] == null || keyFields[i].isEmpty()) {
					throw new RuntimeException("Class resolved at TaskManager is not compatible with class read during Plan setup."
							+ " (" + fieldName + ")");
				}
			}
		}
	}


	public List<Field>[] getKeyFields() {
		return this.keyFields;
	}

	public TypeComparator<Object>[] getComparators() {
		return this.comparators;
	}

	private final Object accessField(List<Field> fieldAccessors, Object object) {
		// We could have another indirection where we have an AccessorProxy for fields with only
		// one element in the accessor chain and another MultiAccessorProxy for fields with several
		// fields to access the final key field. This would add another level of abstraction with additional
		// objects and additional calls per key field so go with this simple version for now.
		for (Field accessor : fieldAccessors) {
			try {
				object = accessor.get(object);
			} catch (NullPointerException npex) {
				List<String> accessorNames = new ArrayList<String>();
				for (Field f : fieldAccessors) {
					accessorNames.add(f.getName());
				}
				Joiner join = Joiner.on('.');
				throw new NullKeyFieldException(join.join(accessorNames));
			} catch (IllegalAccessException iaex) {
				throw new RuntimeException("This should not happen since we call setAccesssible(true) in PojoTypeInfo."
				+ " ACC: " + accessor + " obj: " + object);
			}
		}
		return object;
	}

	@Override
	public int hash(T value) {
		int i = 0;
		int code = 0;
		for (; i < this.keyFields.length; i++) {
			code ^= this.comparators[i].hash(accessField(keyFields[i], value));
			code *= HASH_SALT[i & 0x1F]; // salt code with (i % HASH_SALT.length)-th salt component
		}
		return code;

	}

	@Override
	public void setReference(T toCompare) {
		int i = 0;
		for (; i < this.keyFields.length; i++) {
			this.comparators[i].setReference(accessField(keyFields[i], toCompare));
		}
	}

	@Override
	public boolean equalToReference(T candidate) {
		int i = 0;
		for (; i < this.keyFields.length; i++) {
			if (!this.comparators[i].equalToReference(accessField(keyFields[i], candidate))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int compareToReference(TypeComparator<T> referencedComparator) {
		PojoComparator<T> other = (PojoComparator<T>) referencedComparator;

		int i = 0;
		try {
			for (; i < this.keyFields.length; i++) {
				int cmp = this.comparators[i].compareToReference(other.comparators[i]);
				if (cmp != 0) {
					return cmp;
				}
			}
			return 0;
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException(this.keyFields[i].toString());
		}
	}

	@Override
	public int compare(T first, T second) {
		int i = 0;
		for (; i < keyFields.length; i++) {
			int cmp = comparators[i].compare(accessField(keyFields[i], first), accessField(keyFields[i], second));
			if (cmp != 0) {
				return cmp;
			}
		}

		return 0;
	}

	@Override
	public int compare(DataInputView firstSource, DataInputView secondSource) throws IOException {
		T first = this.serializer.createInstance();
		T second = this.serializer.createInstance();

		first = this.serializer.deserialize(first, firstSource);
		second = this.serializer.deserialize(second, secondSource);

		return this.compare(first, second);
	}

	@Override
	public boolean supportsNormalizedKey() {
		return this.numLeadingNormalizableKeys > 0;
	}

	@Override
	public int getNormalizeKeyLen() {
		return this.normalizableKeyPrefixLen;
	}

	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes) {
		return this.numLeadingNormalizableKeys < this.keyFields.length ||
				this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||
				this.normalizableKeyPrefixLen > keyBytes;
	}

	@Override
	public void putNormalizedKey(T value, MemorySegment target, int offset, int numBytes) {
		int i = 0;
		for (; i < this.numLeadingNormalizableKeys & numBytes > 0; i++)
		{
			int len = this.normalizedKeyLengths[i];
			len = numBytes >= len ? len : numBytes;
			this.comparators[i].putNormalizedKey(accessField(keyFields[i], value), target, offset, len);
			numBytes -= len;
			offset += len;
		}
	}

	@Override
	public boolean invertNormalizedKey() {
		return this.invertNormKey;
	}


	@Override
	public boolean supportsSerializationWithKeyNormalization() {
		return false;
	}

	@Override
	public void writeWithKeyNormalization(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public T readWithKeyDenormalization(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public PojoComparator<T> duplicate() {
		return new PojoComparator<T>(this);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * A sequence of prime numbers to be used for salting the computed hash values.
	 * Based on some empirical evidence, we are using a 32-element subsequence of the
	 * OEIS sequence #A068652 (numbers such that every cyclic permutation is a prime).
	 *
	 * @see: http://en.wikipedia.org/wiki/List_of_prime_numbers
	 * @see: http://oeis.org/A068652
	 */
	private static final int[] HASH_SALT = new int[] {
		73   , 79   , 97   , 113  , 131  , 197  , 199  , 311   ,
		337  , 373  , 719  , 733  , 919  , 971  , 991  , 1193  ,
		1931 , 3119 , 3779 , 7793 , 7937 , 9311 , 9377 , 11939 ,
		19391, 19937, 37199, 39119, 71993, 91193, 93719, 93911 };
}

