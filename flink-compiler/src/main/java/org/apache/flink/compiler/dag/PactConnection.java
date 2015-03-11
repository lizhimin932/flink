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

package org.apache.flink.compiler.dag;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.compiler.dataproperties.InterestingProperties;
import org.apache.flink.compiler.plandump.DumpableConnection;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;

/**
 * A connection between to operators. Represents an intermediate result
 * and a data exchange between the two operators.
 *
 * The data exchange has a mode in which it performs (batch / pipelined)
 *
 * The data exchange strategy may be set on this connection, in which case
 * it is fixed and will not be determined during candidate plan enumeration.
 *
 * During the enumeration of interesting properties, this connection also holds
 * all interesting properties generated by the successor operator.
 */
public class PactConnection implements EstimateProvider, DumpableConnection<OptimizerNode> {
	
	private final OptimizerNode source; // The source node of the connection

	private final OptimizerNode target; // The target node of the connection.

	private final ExecutionMode dataExchangeMode; // defines whether to use batch or pipelined data exchange

	private InterestingProperties interestingProps; // local properties that succeeding nodes are interested in

	private ShipStrategyType shipStrategy; // The data shipping strategy, if predefined.
	
	private TempMode materializationMode = TempMode.NONE; // the materialization mode
	
	private int maxDepth = -1;

	private boolean breakPipeline;  // whether this connection should break the pipeline due to potential deadlocks

	/**
	 * Creates a new Connection between two nodes. The shipping strategy is by default <tt>NONE</tt>.
	 * The temp mode is by default <tt>NONE</tt>.
	 * 
	 * @param source
	 *        The source node.
	 * @param target
	 *        The target node.
	 */
	public PactConnection(OptimizerNode source, OptimizerNode target, ExecutionMode exchangeMode) {
		this(source, target, null, exchangeMode);
	}

	/**
	 * Creates a new Connection between two nodes.
	 * 
	 * @param source
	 *        The source node.
	 * @param target
	 *        The target node.
	 * @param shipStrategy
	 *        The shipping strategy.
	 * @param exchangeMode
	 *        The data exchange mode (pipelined / batch / batch only for shuffles / ... )
	 */
	public PactConnection(OptimizerNode source, OptimizerNode target,
							ShipStrategyType shipStrategy, ExecutionMode exchangeMode)
	{
		if (source == null || target == null) {
			throw new NullPointerException("Source and target must not be null.");
		}
		this.source = source;
		this.target = target;
		this.shipStrategy = shipStrategy;
		this.dataExchangeMode = exchangeMode;
	}
	
	/**
	 * Constructor to create a result from an operator that is not
	 * consumed by another operator.
	 * 
	 * @param source
	 *        The source node.
	 */
	public PactConnection(OptimizerNode source, ExecutionMode exchangeMode) {
		if (source == null) {
			throw new NullPointerException("Source and target must not be null.");
		}
		this.source = source;
		this.target = null;
		this.shipStrategy = ShipStrategyType.NONE;
		this.dataExchangeMode = exchangeMode;
	}

	/**
	 * Gets the source of the connection.
	 * 
	 * @return The source Node.
	 */
	public OptimizerNode getSource() {
		return this.source;
	}

	/**
	 * Gets the target of the connection.
	 * 
	 * @return The target node.
	 */
	public OptimizerNode getTarget() {
		return this.target;
	}

	/**
	 * Gets the shipping strategy for this connection.
	 * 
	 * @return The connection's shipping strategy.
	 */
	public ShipStrategyType getShipStrategy() {
		return this.shipStrategy;
	}

	/**
	 * Sets the shipping strategy for this connection.
	 * 
	 * @param strategy
	 *        The shipping strategy to be applied to this connection.
	 */
	public void setShipStrategy(ShipStrategyType strategy) {
		this.shipStrategy = strategy;
	}

	/**
	 * Gets the data exchange mode to use for this connection.
	 *
	 * @return The data exchange mode to use for this connection.
	 */
	public ExecutionMode getDataExchangeMode() {
		if (dataExchangeMode == null) {
			throw new IllegalStateException("This connection does not have the data exchange mode set");
		}
		return dataExchangeMode;
	}

	/**
	 * Marks that this connection should do a decoupled data exchange (such as batched)
	 * rather then pipeline data. Connections are marked as pipeline breakers to avoid
	 * deadlock situations.
	 */
	public void markBreaksPipeline() {
		this.breakPipeline = true;
	}

	/**
	 * Checks whether this connection is marked to break the pipeline.
	 *
	 * @return True, if this connection is marked to break the pipeline, false otherwise.
	 */
	public boolean isBreakingPipeline() {
		return this.breakPipeline;
	}

	/**
	 * Gets the interesting properties object for this pact connection.
	 * If the interesting properties for this connections have not yet been set,
	 * this method returns null.
	 * 
	 * @return The collection of all interesting properties, or null, if they have not yet been set.
	 */
	public InterestingProperties getInterestingProperties() {
		return this.interestingProps;
	}

	/**
	 * Sets the interesting properties for this pact connection.
	 * 
	 * @param props The interesting properties.
	 */
	public void setInterestingProperties(InterestingProperties props) {
		if (this.interestingProps == null) {
			this.interestingProps = props;
		} else {
			throw new IllegalStateException("Interesting Properties have already been set.");
		}
	}
	
	public void clearInterestingProperties() {
		this.interestingProps = null;
	}
	
	public void initMaxDepth() {
		
		if (this.maxDepth == -1) {
			this.maxDepth = this.source.getMaxDepth() + 1;
		} else {
			throw new IllegalStateException("Maximum path depth has already been initialized.");
		}
	}
	
	public int getMaxDepth() {
		if (this.maxDepth != -1) {
			return this.maxDepth;
		} else {
			throw new IllegalStateException("Maximum path depth has not been initialized.");
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Estimates
	// --------------------------------------------------------------------------------------------

	@Override
	public long getEstimatedOutputSize() {
		return this.source.getEstimatedOutputSize();
	}

	@Override
	public long getEstimatedNumRecords() {
		return this.source.getEstimatedNumRecords();
	}
	
	@Override
	public float getEstimatedAvgWidthPerOutputRecord() {
		return this.source.getEstimatedAvgWidthPerOutputRecord();
	}
	
	// --------------------------------------------------------------------------------------------

	
	public TempMode getMaterializationMode() {
		return this.materializationMode;
	}
	
	public void setMaterializationMode(TempMode materializationMode) {
		this.materializationMode = materializationMode;
	}
	
	public boolean isOnDynamicPath() {
		return this.source.isOnDynamicPath();
	}
	
	public int getCostWeight() {
		return this.source.getCostWeight();
	}

	// --------------------------------------------------------------------------------------------

	public String toString() {
		StringBuilder buf = new StringBuilder(50);
		buf.append("Connection: ");

		if (this.source == null) {
			buf.append("null");
		} else {
			buf.append(this.source.getPactContract().getName());
			buf.append('(').append(this.source.getName()).append(')');
		}

		buf.append(" -> ");

		if (this.shipStrategy != null) {
			buf.append('[');
			buf.append(this.shipStrategy.name());
			buf.append(']').append(' ');
		}

		if (this.target == null) {
			buf.append("null");
		} else {
			buf.append(this.target.getPactContract().getName());
			buf.append('(').append(this.target.getName()).append(')');
		}

		return buf.toString();
	}
}
