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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.sink.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

/**
 * A an abstract base class for different JDBC sinks.  This class takes care of managing
 * connections and transactions.
 */
public abstract class AbstractJDBCSink extends AbstractSink implements Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractJDBCSink.class);
	private CounterGroup counterGroup;
	private int batchSize;
	private SinkCounter counter;
	private JDBCConnectionManager connectionManager;

	/**
	 * This method is called after the transaction is started but before event processing
	 * starts.  It allows subclasses to get ready for event processing.
	 * @throws SQLException
	 */
	protected abstract void prepareJDBC() throws SQLException;
	
	/**
	 * This method is called for each incoming event.  Subclasses are responsible for
	 * obtaining the database connection and doing the JDBC operations required to
	 * jam this event into the database.
	 * @param event the event to process
	 * @throws Exception on any sort of processing problem - connection, conversion, etc
	 */
	protected abstract void processJDBC(final Event event) throws Exception;
	
	/**
	 * This method is called before the transaction is committed but after event processing
	 * is completed.  It allows subclasses to get finish or clean up afterwards.
	 * @throws SQLException
	 */
	protected abstract void completeJDBC() throws SQLException;

	/**
	 * This method is called on error, before rollback and failure to give the subclass an
	 * attempt to clean up.
	 * @throws SQLException
	 */
	protected abstract void abortJDBC() throws SQLException;

	@Override
	public void configure(final Context context) {
		if (counter == null) {
			counter = new SinkCounter(getName());
		}
		if (counterGroup == null) {
			counterGroup = new CounterGroup();
		}

		connectionManager = new JDBCConnectionManager(counter);
		connectionManager.configure(context);
		
		batchSize = context.getInteger("batchSize", 100);
		Preconditions.checkArgument(batchSize > 0, "Batch size must be specified and greater than zero.");
	}

	@Override
	public synchronized void start() {
		super.start();
		counter.start();
		connectionManager.start();
	}

	@Override
	public synchronized void stop() {
		super.stop();
		connectionManager.closeConnection();
		counter.stop();
	}
	
	/**
	 * Gets the current JDBC database connection.
	 * @return the connection
	 */
	public Connection getConnection() {
		return connectionManager.getConnection();
	}

	@Override
	public Status process() throws EventDeliveryException {
		final Channel channel = getChannel();
		final Transaction transaction = channel.getTransaction();
		
		try {
			// Start transactions, prepare for JDBC operations.
			transaction.begin();
			connectionManager.ensureConnectionValid();
			prepareJDBC();

			// For each event...
			int count;
			for (count = 0; count < batchSize; count++) {
				final Event event = channel.take();

				if (event == null) {
					break;
				}

				processJDBC(event);
			}
			
			// Clean up.
			completeJDBC();

			// Update attempt counters.  Commit.  Update success counters.
			final Status status = updateAttemptCounters(count);
			connectionManager.getConnection().commit();
			transaction.commit();
			updateSuccessCounters(count);
			return status;
		} catch (Exception e) {

			try {
				// Something went wrong, back out.  Update failure counters.
				abortJDBC();
				connectionManager.getConnection().rollback();
				transaction.rollback();
				updateFailureCounters();
			} catch (Exception e2) {
				LOG.error(
						"Exception in rollback. Rollback might not have been successful.",
						e2);
			}

			LOG.error("Failed to commit transaction. Transaction rolled back.",
					e);
			Throwables.propagate(e);
		} finally {
			transaction.close();
		}
		
		// This should never happen.
		return null;
	}
	
	@VisibleForTesting
	void setConnectionManager(final JDBCConnectionManager connectionManager) {
		this.connectionManager = connectionManager;
	}
	
	/**
	 * Update the attempt counters.  (As a side effect, return the processing status.
	 * This is only done here for convenience.)
	 * @param count the number of events attempted
	 * @return the resulting sink status
	 */
	private Status updateAttemptCounters(final int count) {
		counter.addToEventDrainAttemptCount(count);

		if (count == 0) {
			counter.incrementBatchEmptyCount();
			counterGroup.incrementAndGet("channel.underflow");
			return Status.BACKOFF;
		}
		
		if (count < batchSize) {
			counter.incrementBatchUnderflowCount();
			return Status.BACKOFF;
		}
		
		// Else, count == batchSize and the batch is full.
		counter.incrementBatchCompleteCount();
		return Status.READY;
	}

	/**
	 * Update success counters
	 * @param count the number of events successfully processed
	 */
	private void updateSuccessCounters(final int count) {
		counter.addToEventDrainSuccessCount(count);
		counterGroup.incrementAndGet("transaction.success");
	}

	/**
	 * Update failure counters.  Increments the transaction rollback counter.
	 */
	private void updateFailureCounters() {
		counterGroup.incrementAndGet("transaction.rollback");
	}

}
