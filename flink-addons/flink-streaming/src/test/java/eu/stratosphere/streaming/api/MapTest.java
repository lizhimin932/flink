package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import org.junit.Test;

import eu.stratosphere.api.datastream.DataStream;
import eu.stratosphere.api.datastream.StreamExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamcomponent.StreamInvokableComponent;
import eu.stratosphere.util.Collector;

public class MapTest {

	public static final class MyMap extends MapFunction<Tuple1<String>, Tuple1<String>> {
		@Override
		public Tuple1<String> map(Tuple1<String> value) throws Exception {
			System.out.println("map");
			return value;
		}
	}

	@Test
	public void test() throws Exception {
		Tuple1<String> tup = new Tuple1<String>("asd");

		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		DataStream<Tuple1<String>> dataStream = context.setDummySource().map(new MyMap())
				.addDummySink();

		context.execute();

		JobGraphBuilder jgb = context.jobGB();

		for (AbstractJobVertex c : jgb.components.values()) {
			if (c instanceof JobTaskVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				MapFunction<Tuple, Tuple> f = (MapFunction<Tuple, Tuple>) in.readObject();

				StreamCollector<Tuple> s = new StreamCollector<Tuple>(1, 1, null);
				Tuple t = new Tuple1<String>("asd");

				s.collect(f.map(t));

				System.out.println(f.getClass().getGenericSuperclass());
				TupleTypeInfo<Tuple> ts = (TupleTypeInfo) TypeExtractor.createTypeInfo(
						MapFunction.class, f.getClass(), 0, null, null);

				System.out.println(ts);

				byte[] userFunctionSerialized = config.getBytes("serializedudf", null);
				in = new ObjectInputStream(new ByteArrayInputStream(userFunctionSerialized));
				StreamInvokableComponent userFunction = (StreamInvokableComponent) in.readObject();
				System.out.println(userFunction.getClass());
				assertTrue(true);
				System.out.println("----------------");
			}

			if (c instanceof JobOutputVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				UserSinkInvokable<Tuple> f = (UserSinkInvokable<Tuple>) in.readObject();

				System.out.println(f.getClass().getGenericSuperclass());
				TupleTypeInfo<Tuple> ts = (TupleTypeInfo) TypeExtractor.createTypeInfo(
						UserSinkInvokable.class, f.getClass(), 0, null, null);

				System.out.println(ts);

				byte[] userFunctionSerialized = config.getBytes("serializedudf", null);
				in = new ObjectInputStream(new ByteArrayInputStream(userFunctionSerialized));
				UserSinkInvokable userFunction = (UserSinkInvokable) in.readObject();
				System.out.println(userFunction.getClass());
				assertTrue(true);
				System.out.println("----------------");
			}

			if (c instanceof JobInputVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				UserSourceInvokable<Tuple> f = (UserSourceInvokable<Tuple>) in.readObject();

				System.out.println(f.getClass().getGenericSuperclass());
				TupleTypeInfo<Tuple> ts = (TupleTypeInfo) TypeExtractor.createTypeInfo(
						UserSourceInvokable.class, f.getClass(), 0, null, null);

				System.out.println(ts);
				System.out.println("----------------");
			}
		}
	}
}
