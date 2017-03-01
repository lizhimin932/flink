package org.apache.flink.table.plan.nodes.datastream.function;

import java.io.Serializable;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.plan.nodes.datastream.aggs.AnyCounterAggregator;
import org.apache.flink.table.plan.nodes.datastream.aggs.DoubleSummaryAggregation;
import org.apache.flink.table.plan.nodes.datastream.aggs.IntegerSummaryAggregation;
import org.apache.flink.table.plan.nodes.datastream.aggs.LongSummaryAggregation;
import org.apache.flink.table.plan.nodes.datastream.aggs.StreamAggregator;

public abstract class DataStreamProcTimeWindowAggregator implements Serializable{

	List<String> aggregators;
	List<Integer> indexes;
	List<TypeInformation<?>> typeInput;
	List<TypeInformation<?>> typeOutput;
	@SuppressWarnings("rawtypes")
	List<StreamAggregator> aggregatorImpl;
	
	
	protected void setAggregator(int i, String aggregationName) {

		if (aggregationName.toLowerCase().contains("sum")) {
			setSumAggregation(i);

		} else {
			if (aggregationName.toLowerCase().contains("count")) {
				setCountAggregation(i);
			} else if (aggregationName.toLowerCase().contains("min")) {
				setMinAggregation(i);
			} else if (aggregationName.toLowerCase().contains("max")) {
				setMaxAggregation(i);
			} else if (aggregationName.toLowerCase().contains("avg")) {
				setAvgAggregation(i);
			} else {
				throw new IllegalArgumentException("Unsupported aggregation");
			}
		}

	}

	private void setMinAggregation(int i) {
		if (typeOutput.get(i).getTypeClass().equals(Integer.class)) {
			aggregatorImpl.add(new IntegerSummaryAggregation().initMin());
		} else {
			if (typeOutput.get(i).getTypeClass().equals(Long.class)) {
				aggregatorImpl.add(new LongSummaryAggregation().initMin());
			} else {
				if (typeOutput.get(i).getTypeClass().equals(Double.class)) {
					aggregatorImpl.add(new DoubleSummaryAggregation().initMin());
				} else {
					throw new IllegalArgumentException("Unsupported aggregation type");
				}
			}
		}
	}

	private void setMaxAggregation(int i) {
		if (typeOutput.get(i).getTypeClass().equals(Integer.class)) {
			aggregatorImpl.add(new IntegerSummaryAggregation().initMax());
		} else {
			if (typeOutput.get(i).getTypeClass().equals(Long.class)) {
				aggregatorImpl.add(new LongSummaryAggregation().initMax());
			} else {
				if (typeOutput.get(i).getTypeClass().equals(Double.class)) {
					aggregatorImpl.add(new DoubleSummaryAggregation().initMax());
				} else {
					throw new IllegalArgumentException("Unsupported aggregation type");
				}
			}
		}
	}

	private void setAvgAggregation(int i) {

		// output of average is double
		if (typeOutput.get(i).getTypeClass().equals(Double.class)) {
			throw new IllegalArgumentException("Unsupported aggregation type");
		}

		if (typeInput.get(i).getTypeClass().equals(Integer.class)) {
			aggregatorImpl.add(new IntegerSummaryAggregation().initAvg());
		} else {
			if (typeInput.get(i).getTypeClass().equals(Long.class)) {
				aggregatorImpl.add(new LongSummaryAggregation().initAvg());
			} else {
				if (typeInput.get(i).getTypeClass().equals(Double.class)) {
					aggregatorImpl.add(new DoubleSummaryAggregation().initAvg());
				} else {
					throw new IllegalArgumentException("Unsupported aggregation type");
				}
			}
		}
	}

	private void setCountAggregation(int i) {

		if (typeOutput.get(i).getTypeClass().equals(Long.class)) {
			aggregatorImpl.add(new AnyCounterAggregator());
		} else {
			throw new IllegalArgumentException("Unsupported aggregation type");
		}

	}

	private void setSumAggregation(int i) {
		if (typeOutput.get(i).getTypeClass().equals(Integer.class)) {
			aggregatorImpl.add(new IntegerSummaryAggregation().initSum());
		} else {
			if (typeOutput.get(i).getTypeClass().equals(Long.class)) {
				aggregatorImpl.add(new LongSummaryAggregation().initSum());
			} else {
				if (typeOutput.get(i).getTypeClass().equals(Double.class)) {
					aggregatorImpl.add(new DoubleSummaryAggregation().initSum());
				} else {
					throw new IllegalArgumentException("Unsupported aggregation type");
				}
			}
		}
	}
	
}
