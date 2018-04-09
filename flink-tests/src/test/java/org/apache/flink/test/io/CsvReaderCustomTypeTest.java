package org.apache.flink.test.io;

import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.test.io.csv.custom.type.NestedCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.NestedCustomJsonTypeStringParser;
import org.apache.flink.test.io.csv.custom.type.simple.SimpleCustomJsonType;
import org.apache.flink.test.io.csv.custom.type.simple.SimpleCustomJsonTypeStringParser;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.parser.FieldParser;
import org.apache.flink.types.parser.ParserFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * A parent class for integration test container classes for CsvReader.
 */
@RunWith(Parameterized.class)
public abstract class CsvReaderCustomTypeTest extends MultipleProgramsTestBase {

	protected CsvReaderContext context;

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Before
	public void setUp() {
		context = new CsvReaderContext();
	}

	@After
	public void tearDown() {
		context = null;
	}

	public CsvReaderCustomTypeTest(TestExecutionMode mode) {
		super(mode);
	}

	/**
	 * Execution context for the Given-When-Then paradigm.
	 */
	protected static class CsvReaderContext {

		protected String sourceData;
		protected CsvReader reader;
		protected DataSource<?> dataSource;
		protected List<?> result;

	}

	static final class NestedCustomJsonParserFactory implements ParserFactory<NestedCustomJsonType> {

		@Override
		public Class<? extends FieldParser<NestedCustomJsonType>> getParserType() {
			return NestedCustomJsonTypeStringParser.class;
		}

		@Override
		public FieldParser<NestedCustomJsonType> create() {
			return new NestedCustomJsonTypeStringParser();
		}
	}

	static final class SimpleCustomJsonParserFactory implements ParserFactory<SimpleCustomJsonType> {

		@Override
		public Class<? extends FieldParser<SimpleCustomJsonType>> getParserType() {
			return SimpleCustomJsonTypeStringParser.class;
		}

		@Override
		public FieldParser<SimpleCustomJsonType> create() {
			return new SimpleCustomJsonTypeStringParser();
		}
	}
}


