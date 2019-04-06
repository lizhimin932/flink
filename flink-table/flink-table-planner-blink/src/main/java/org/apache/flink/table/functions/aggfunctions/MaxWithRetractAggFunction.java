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

package org.apache.flink.table.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.typeutils.DecimalTypeInfo;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * built-in Max with retraction aggregate function.
 */
public abstract class MaxWithRetractAggFunction<T>
		extends AggregateFunction<T, MaxWithRetractAggFunction.MaxWithRetractAccumulator<T>> {

	/** The initial accumulator for Max with retraction aggregate function. */
	public static class MaxWithRetractAccumulator<T> {
		public T max;
		public Long distinctCount;
		public MapView<T, Long> map;
	}

	@Override
	public MaxWithRetractAccumulator<T> createAccumulator() {
		MaxWithRetractAccumulator<T> acc = new MaxWithRetractAccumulator<>();
		acc.max = getInitValue(); // max
		acc.distinctCount = 0L;
		// store the count for each value
		acc.map = new MapView<>(getValueTypeInfo(), BasicTypeInfo.LONG_TYPE_INFO);
		return acc;
	}

	public void accumulate(MaxWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;

			if (acc.distinctCount == 0L || getComparator().compare(acc.max, v) < 0) {
				acc.max = v;
			}

			Long count = acc.map.get(v);
			if (count == null) {
				acc.map.put(v, 1L);
				acc.distinctCount += 1;
			} else {
				count += 1L;
				acc.map.put(v, count);
			}
		}
	}

	public void retract(MaxWithRetractAccumulator<T> acc, Object value) throws Exception {
		if (value != null) {
			T v = (T) value;

			Long count = acc.map.get(v);
			if (count == null || count == 1L) {
				//remove the key v from the map if the number of appearance of the value v is 0
				if (count != null) {
					acc.map.remove(v);
				}
				//if the total count is 0, we could just simply set the f0(max) to the initial value
				acc.distinctCount -= 1L;
				if (acc.distinctCount == 0L) {
					acc.max = getInitValue();
					return;
				}
				//if v is the current max value, we have to iterate the map to find the 2nd biggest
				// value to replace v as the max value
				if (v == acc.max) {
					Iterator<T> iterator = acc.map.keys().iterator();
					boolean hasMax = false;
					Comparator<T> comparator = getComparator();
					while (iterator.hasNext()) {
						T key = iterator.next();
						if (!hasMax || comparator.compare(acc.max, key) < 0) {
							acc.max = key;
							hasMax = true;
						}
					}
					if (!hasMax) {
						acc.distinctCount = 0L;
					}
				}
			} else {
				acc.map.put(v, count - 1);
			}
		}
	}

	public void merge(MaxWithRetractAccumulator<T> acc, Iterable<MaxWithRetractAccumulator<T>> its) throws Exception {
		Iterator<MaxWithRetractAccumulator<T>> iter = its.iterator();
		Comparator<T> comparator = getComparator();
		while (iter.hasNext()) {
			MaxWithRetractAccumulator<T> a = iter.next();
			if (a.distinctCount != 0) {
				// set max element
				if (comparator.compare(acc.max, a.max) < 0) {
					acc.max = a.max;
				}
				// merge the count for each key
				Iterator<Map.Entry<T, Long>> iterator = a.map.entries().iterator();
				while (iterator.hasNext()) {
					Map.Entry entry = iterator.next();
					T key = (T) entry.getKey();
					Long value = (Long) entry.getValue();
					Long count = acc.map.get(key);
					if (count != null) {
						acc.map.put(key, count + value);
					} else {
						acc.map.put(key, value);
						acc.distinctCount += 1;
					}
				}
			}
		}
	}

	public void resetAccumulator(MaxWithRetractAccumulator<T> acc) {
		acc.max = getInitValue();
		acc.distinctCount = 0L;
		acc.map.clear();
	}

	@Override
	public T getValue(MaxWithRetractAccumulator<T> acc) {
		if (acc.distinctCount != 0) {
			return acc.max;
		} else {
			return null;
		}
	}

	protected abstract T getInitValue();

	protected abstract TypeInformation<?> getValueTypeInfo();

	protected abstract Comparator<T> getComparator();

	/**
	 * Built-in Byte Max with retraction aggregate function.
	 */
	public static class ByteMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Byte> {

		@Override
		protected Byte getInitValue() {
			return (byte) 0;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.BYTE_TYPE_INFO;
		}

		@Override
		protected Comparator<Byte> getComparator() {
			return new Comparator<Byte>() {
				@Override
				public int compare(Byte o1, Byte o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Short Max with retraction aggregate function.
	 */
	public static class ShortMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Short> {

		@Override
		protected Short getInitValue() {
			return (short) 0;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.SHORT_TYPE_INFO;
		}

		@Override
		protected Comparator<Short> getComparator() {
			return new Comparator<Short>() {
				@Override
				public int compare(Short o1, Short o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Int Max with retraction aggregate function.
	 */
	public static class IntMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Integer> {

		@Override
		protected Integer getInitValue() {
			return 0;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.INT_TYPE_INFO;
		}

		@Override
		protected Comparator<Integer> getComparator() {
			return new Comparator<Integer>() {
				@Override
				public int compare(Integer o1, Integer o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Long Max with retraction aggregate function.
	 */
	public static class LongMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Long> {

		@Override
		protected Long getInitValue() {
			return 0L;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.LONG_TYPE_INFO;
		}

		@Override
		protected Comparator<Long> getComparator() {
			return new Comparator<Long>() {
				@Override
				public int compare(Long o1, Long o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Float Max with retraction aggregate function.
	 */
	public static class FloatMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Float> {

		@Override
		protected Float getInitValue() {
			return 0.0f;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.FLOAT_TYPE_INFO;
		}

		@Override
		protected Comparator<Float> getComparator() {
			return new Comparator<Float>() {
				@Override
				public int compare(Float o1, Float o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Double Max with retraction aggregate function.
	 */
	public static class DoubleMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Double> {

		@Override
		protected Double getInitValue() {
			return 0.0D;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.DOUBLE_TYPE_INFO;
		}

		@Override
		protected Comparator<Double> getComparator() {
			return new Comparator<Double>() {
				@Override
				public int compare(Double o1, Double o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Boolean Max with retraction aggregate function.
	 */
	public static class BooleanMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Boolean> {

		@Override
		protected Boolean getInitValue() {
			return false;
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.BOOLEAN_TYPE_INFO;
		}

		@Override
		protected Comparator<Boolean> getComparator() {
			return new Comparator<Boolean>() {
				@Override
				public int compare(Boolean o1, Boolean o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Big Decimal Max with retraction aggregate function.
	 */
	public static class DecimalMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Decimal> {
		private DecimalTypeInfo decimalType;

		public DecimalMaxWithRetractAggFunction(DecimalTypeInfo decimalType) {
			this.decimalType = decimalType;
		}

		@Override
		protected Decimal getInitValue() {
			return Decimal.castFrom(0, decimalType.precision(), decimalType.scale());
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return decimalType;
		}

		@Override
		protected Comparator<Decimal> getComparator() {
			return new Comparator<Decimal>() {
				@Override
				public int compare(Decimal o1, Decimal o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in String Max with retraction aggregate function.
	 */
	public static class StringMaxWithRetractAggFunction extends MaxWithRetractAggFunction<String> {

		@Override
		protected String getInitValue() {
			return "";
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return BasicTypeInfo.STRING_TYPE_INFO;
		}

		@Override
		protected Comparator<String> getComparator() {
			return new Comparator<String>() {
				@Override
				public int compare(String o1, String o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Timestamp Max with retraction aggregate function.
	 */
	public static class TimestampMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Timestamp> {

		@Override
		protected Timestamp getInitValue() {
			return new Timestamp(0);
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_TIMESTAMP;
		}

		@Override
		protected Comparator<Timestamp> getComparator() {
			return new Comparator<Timestamp>() {
				@Override
				public int compare(Timestamp o1, Timestamp o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Date Max with retraction aggregate function.
	 */
	public static class DateMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Date> {

		@Override
		protected Date getInitValue() {
			return new Date(0);
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_DATE;
		}

		@Override
		protected Comparator<Date> getComparator() {
			return new Comparator<Date>() {
				@Override
				public int compare(Date o1, Date o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}

	/**
	 * Built-in Time Max with retraction aggregate function.
	 */
	public static class TimeMaxWithRetractAggFunction extends MaxWithRetractAggFunction<Time> {

		@Override
		protected Time getInitValue() {
			return new Time(0);
		}

		@Override
		protected TypeInformation<?> getValueTypeInfo() {
			return Types.SQL_TIME;
		}

		@Override
		protected Comparator<Time> getComparator() {
			return new Comparator<Time>() {
				@Override
				public int compare(Time o1, Time o2) {
					return o1.compareTo(o2);
				}
			};
		}
	}
}
