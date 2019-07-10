package org.apache.flink.table.sinks;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.table.utils.TableConnectorUtils;

/**
 * A simple {@link TableSink} to emit data to the standard output stream.
 */
public class PrintTableSink implements BatchTableSink , AppendStreamTableSink {

	private String[] fieldNames;
	private TypeInformation<?>[] fieldTypes;

	@Override
	public void emitDataSet(DataSet dataSet) {
		try {
			dataSet.print();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void emitDataStream(DataStream dataStream) {
		consumeDataStream(dataStream);
	}

	@Override
	public DataStreamSink<?> consumeDataStream(DataStream dataStream) {

		DataStreamSink sink = dataStream.addSink(new PrintSinkFunction());
		sink.name(TableConnectorUtils.generateRuntimeName(PrintTableSink.class, fieldNames));
		return sink;
	}

	@Override
	public TypeInformation getOutputType() {
		return new RowTypeInfo(getFieldTypes(), getFieldNames());
	}

	@Override
	public String[] getFieldNames() {
		return fieldNames;
	}

	@Override
	public TypeInformation<?>[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public TableSink configure(String[] fieldNames, TypeInformation[] fieldTypes) {
		PrintTableSink configuredSink = new PrintTableSink();
		configuredSink.fieldNames = fieldNames;
		configuredSink.fieldTypes = fieldTypes;
		return configuredSink;
	}
}
