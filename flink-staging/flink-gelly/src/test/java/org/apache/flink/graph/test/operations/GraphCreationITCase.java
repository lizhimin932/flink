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

package org.apache.flink.graph.test.operations;

import com.google.common.base.Charsets;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.graph.test.TestGraphUtils.DummyCustomParameterizedType;
import org.apache.flink.graph.validation.InvalidVertexIdsValidator;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

@RunWith(Parameterized.class)
public class GraphCreationITCase extends MultipleProgramsTestBase {

	public GraphCreationITCase(TestExecutionMode mode){
		super(mode);
	}

	private static final Path PATH = new Path("an/ignored/file/");
    private String resultPath;
    private String expectedResult;

    @Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void before() throws Exception{
		resultPath = tempFolder.newFile().toURI().toString();
	}

	@After
	public void after() throws Exception{
		compareResultsByLinesInMemory(expectedResult, resultPath);
	}

	@Test
	public void testCreateWithoutVertexValues() throws Exception {
	/*
	 * Test create() with edge dataset and no vertex values
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, NullValue, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongEdgeData(env), env);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,(null)\n" +
					"2,(null)\n" +
					"3,(null)\n" +
					"4,(null)\n" +
					"5,(null)\n";
	}

	@Test
	public void testCreateWithMapper() throws Exception {
	/*
	 * Test create() with edge dataset and a mapper that assigns the id as value
     */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, Long, Long> graph = Graph.fromDataSet(TestGraphUtils.getLongLongEdgeData(env),
				new AssignIdAsValueMapper(), env);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,1\n" +
					"2,2\n" +
					"3,3\n" +
					"4,4\n" +
					"5,5\n";
	}

	@Test
	public void testCreateWithCustomVertexValue() throws Exception {
		/*
		 * Test create() with edge dataset and a mapper that assigns a parametrized custom vertex value
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		Graph<Long, DummyCustomParameterizedType<Double>, Long> graph = Graph.fromDataSet(
				TestGraphUtils.getLongLongEdgeData(env), new AssignCustomVertexValueMapper(), env);

		graph.getVertices().writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,(2.0,0)\n" +
				"2,(4.0,1)\n" +
				"3,(6.0,2)\n" +
				"4,(8.0,3)\n" +
				"5,(10.0,4)\n";
	}

	@Test
	public void testCreateWithCsvFile() throws Exception {
		/*
		 * Test with two Csv files one with Vertex Data and one with Edges data
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		final String fileContent =  "1,1\n"+
									"2,2\n"+
									"3,3\n";
		final FileInputSplit split = createTempFile(fileContent);
		final String fileContent2 =  "1,2,ot\n"+
									"3,2,tt\n"+
									"3,1,to\n";
		final FileInputSplit split2 = createTempFile(fileContent2);
		Graph<Long,Long,String> graph= Graph.fromCsvReader(split.getPath().toString(),split2.getPath().toString(),env).types(Long.class,Long.class,String.class);
		graph.getTriplets().writeAsCsv(resultPath);
		env.execute();
		expectedResult = "1,2,1,2,ot\n" +
				"3,2,3,2,tt\n" +
				"3,1,3,1,to\n";
	}

	@Test
	public void testValidate() throws Exception {
		/*
		 * Test validate():
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Vertex<Long, Long>> vertices = TestGraphUtils.getLongLongVertexData(env);
		DataSet<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdgeData(env);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, edges, env);
		Boolean result = graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());

		env.fromElements(result).writeAsText(resultPath);
		env.execute();

		expectedResult = "true\n";
	}

	@Test
	public void testValidateWithInvalidIds() throws Exception {
		/*
		 * Test validate() - invalid vertex ids
		 */
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Vertex<Long, Long>> vertices = TestGraphUtils.getLongLongInvalidVertexData(env);
		DataSet<Edge<Long, Long>> edges = TestGraphUtils.getLongLongEdgeData(env);

		Graph<Long, Long, Long> graph = Graph.fromDataSet(vertices, edges, env);
		Boolean result = graph.validate(new InvalidVertexIdsValidator<Long, Long, Long>());
		env.fromElements(result).writeAsText(resultPath);
		env.execute();

		expectedResult = "false\n";
	}

	@SuppressWarnings("serial")
	private static final class AssignIdAsValueMapper implements MapFunction<Long, Long> {
		public Long map(Long vertexId) {
			return vertexId;
		}
	}

	@SuppressWarnings("serial")
	private static final class AssignCustomVertexValueMapper implements
		MapFunction<Long, DummyCustomParameterizedType<Double>> {

		DummyCustomParameterizedType<Double> dummyValue =
				new DummyCustomParameterizedType<Double>();

		public DummyCustomParameterizedType<Double> map(Long vertexId) {
			dummyValue.setIntField(vertexId.intValue()-1);
			dummyValue.setTField(vertexId*2.0);
			return dummyValue;
		}
	}

	private FileInputSplit createTempFile(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();

		OutputStreamWriter wrt = new OutputStreamWriter(
				new FileOutputStream(tempFile), Charsets.UTF_8
		);
		wrt.write(content);
		wrt.close();

		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[] {"localhost"});
	}
}