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

package org.apache.flink.runtime.jobmanager.web;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.event.job.AbstractEvent;
import org.apache.flink.runtime.event.job.RecentJobEvent;
import org.apache.flink.runtime.jobgraph.JobID;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.profiling.ProfilingUtils;
import org.apache.flink.runtime.profiling.types.InstanceProfilingEvent;
import org.apache.flink.runtime.profiling.types.InstanceSummaryProfilingEvent;
import org.apache.flink.runtime.profiling.types.ProfilingEvent;
import org.apache.flink.runtime.profiling.types.SingleInstanceProfilingEvent;
import org.apache.flink.util.StringUtils;

/**
 * This servlet delivers information on profiling events to the client.
 * 
 * Supported requests:
 * <ol>
 * <li><tt>get=setttings</tt>: Frontend profiling configuration</li>
 * <li><tt>get=profilingEvents</tt> or no <tt>get</tt>: Deliver current
 * profiling events for a/the current job (use <tt>jobid</tt> to specificy job
 * ID)</li>
 * </ol>
 */
public class ResourceUsageServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	public static final String PROFILING_RESULTS_DISPLAY_WINDOW_SIZE_KEY = "jobmanager.web.profiling.windowsize";

	/**
	 * For debugging purposes: Deliver also unused profiling events to the
	 * client?
	 */
	private static final boolean PRINT_ALL_EVENTS = false;

	/**
	 * The log for this class.
	 */
	private static final Log LOG = LogFactory.getLog(ResourceUsageServlet.class);

	private final Map<Class<? extends ProfilingEvent>, ProfilingEventSerializer<? extends ProfilingEvent>> jsonSerializers = new HashMap<Class<? extends ProfilingEvent>, ResourceUsageServlet.ProfilingEventSerializer<?>>();

	private final JobManager jobManager;

	public ResourceUsageServlet(JobManager jobManager) {
		this.jobManager = jobManager;

		this.jsonSerializers.put(InstanceSummaryProfilingEvent.class, new InstanceSummaryProfilingEventSerializer());
		this.jsonSerializers.put(SingleInstanceProfilingEvent.class, new SingleInstanceProfilingEventSerializer());
	}

	@Override
	protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		String getParameter = req.getParameter("get"); // profilingEvents is
														// default
		if (getParameter == null) {
			respondProfilingEvents(req, resp);
		} else if (getParameter.equalsIgnoreCase("profilingEvents")) {
			respondProfilingEvents(req, resp);
		} else if (getParameter.equalsIgnoreCase("settings")) {
			respondSettings(req, resp);
		}
	}

	/**
	 * Reports the configuration for the profiling frontend.
	 */
	private void respondSettings(HttpServletRequest req, HttpServletResponse resp) throws IOException {
		long windowSize = GlobalConfiguration.getLong(PROFILING_RESULTS_DISPLAY_WINDOW_SIZE_KEY, -1);
		boolean isProfilingEnabled = GlobalConfiguration.getBoolean(ProfilingUtils.ENABLE_PROFILING_KEY, false);
		resp.setStatus(HttpServletResponse.SC_OK);
		resp.setContentType("application/json");
		PrintWriter writer = resp.getWriter();
		writer.format("{\"windowSize\":%d,\"enable\":%b}", windowSize, isProfilingEnabled);
	}

	/**
	 * Gathers {@link ProfilingEvent} objects from the {@link JobManager} and reports them in JSON format.
	 */
	private void respondProfilingEvents(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
		try {
			JobID jobID = getJobID(req);

			List<AbstractEvent> allJobEvents = jobID == null ? Collections.<AbstractEvent> emptyList() : this.jobManager.getEvents(jobID);

			resp.setStatus(HttpServletResponse.SC_OK);
			resp.setContentType("application/json");
			resp.getWriter().write("[");

			String separator = "";
			for (AbstractEvent jobEvent : allJobEvents) {
				if (jobEvent instanceof ProfilingEvent) {
					ProfilingEvent profilingEvent = (ProfilingEvent) jobEvent;
					ProfilingEventSerializer<ProfilingEvent> jsonSerializer = getSerializer(profilingEvent);
					if (jsonSerializer != null) {
						resp.getWriter().write(separator);
						jsonSerializer.write(profilingEvent, resp.getWriter());
						separator = ",";
					} else if (PRINT_ALL_EVENTS) {
						// This is not necessary and only useful to see what
						// events are actually available in the frontend.
						resp.getWriter().write(separator);
						new ProfilingEventSerializer<ProfilingEvent>().write(profilingEvent, resp.getWriter());
						separator = ",";
					}
				}

			}

			resp.getWriter().write("]");

		} catch (Exception e) {
			resp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			resp.setContentType("text/html");
			resp.getWriter().print(e.getMessage());
			if (LOG.isWarnEnabled()) {
				LOG.warn(StringUtils.stringifyException(e));
			}
		}
	}

	/**
	 * Find a suitable serializer for the given {@link ProfilingEvent} as registered in {@link #jsonSerializers}.
	 * @param profilingEvent is the event that shall be serialized
	 * @return a suitable serializer or <tt>null</tt> if none exists
	 */
	@SuppressWarnings("unchecked")
	private ProfilingEventSerializer<ProfilingEvent> getSerializer(ProfilingEvent profilingEvent) {
		return (ProfilingEventSerializer<ProfilingEvent>) this.jsonSerializers.get(profilingEvent.getClass());
	}

	/** Loads the job ID from the request or selects the latest submitted job. */
	private JobID getJobID(HttpServletRequest req) throws IOException {
		String jobIdParameter = req.getParameter("jobid");
		JobID jobID = jobIdParameter == null ? loadLatestJobID() : JobID.fromHexString(jobIdParameter);
		return jobID;
	}

	/**
	 * @return the latest job ID from the {@link #jobManager}.
	 * @throws IOException
	 *             if there is a problem with retrieving the job list
	 */
	private JobID loadLatestJobID() throws IOException {
		List<RecentJobEvent> recentJobEvents = this.jobManager.getRecentJobs();
		RecentJobEvent mostRecentJobEvent = null;
		for (RecentJobEvent jobEvent : recentJobEvents) {
			if (mostRecentJobEvent == null || mostRecentJobEvent.getSubmissionTimestamp() < jobEvent.getSubmissionTimestamp()) {
				mostRecentJobEvent = jobEvent;
			}
		}
		return mostRecentJobEvent == null ? null : mostRecentJobEvent.getJobID();
	}

	/**
	 * Abstract class with some convenience functionality to serialize
	 * {@link ProfilingEvent} objects to JSON.
	 */
	private static class ProfilingEventSerializer<T extends ProfilingEvent> {

		private PrintWriter writer;
		private String separator;

		/**
		 * Write a string parameter to the current {@link #writer}.
		 */
		protected void writeField(String name, String value) {
			this.writer.write(separator);
			this.writer.write("\"");
			this.writer.write(name);
			this.writer.write("\":\"");
			this.writer.write(value);
			this.writer.write("\"");
			this.separator = ",";
		}

		/**
		 * Write a long parameter to the current {@link #writer}.
		 */
		protected void writeField(String name, long value) {
			this.writer.write(separator);
			this.writer.write("\"");
			this.writer.write(name);
			this.writer.write("\":");
			this.writer.write(Long.toString(value));
			this.separator = ",";
		}

		protected void writeFields(T profilingEvent) {
			writeField("type", profilingEvent.getClass().getSimpleName());
			writeField("jobID", profilingEvent.getJobID().toString());
			writeField("timestamp", profilingEvent.getTimestamp());
		}

		public synchronized void write(T profilingEvent, PrintWriter writer) throws IOException {
			this.writer = writer; // cache the writer for convenience -- we are
									// synchronized here
			this.writer.write("{");
			this.separator = "";
			writeFields(profilingEvent);
			this.writer.write("}");
			this.writer = null;
		}

	}

	private static class InstanceProfilingEventSerializer<T extends InstanceProfilingEvent> extends ProfilingEventSerializer<T> {

		@Override
		protected void writeFields(T profilingEvent) {
			super.writeFields(profilingEvent);
			writeField("userCpu", profilingEvent.getUserCPU());
			writeField("systemCpu", profilingEvent.getSystemCPU());
			writeField("ioWaitCpu", profilingEvent.getIOWaitCPU());
			writeField("softIrqCpu", profilingEvent.getSoftIrqCPU());
			writeField("hardIrqCpu", profilingEvent.getHardIrqCPU());
			writeField("totalMemory", profilingEvent.getTotalMemory());
			writeField("freeMemory", profilingEvent.getFreeMemory());
			writeField("bufferedMemory", profilingEvent.getBufferedMemory());
			writeField("cachedMemory", profilingEvent.getCachedMemory());
			writeField("cachedSwapMemory", profilingEvent.getCachedSwapMemory());
			writeField("transmittedBytes", profilingEvent.getTransmittedBytes());
			writeField("receivedBytes", profilingEvent.getReceivedBytes());
		}
	}

	private static class InstanceSummaryProfilingEventSerializer extends InstanceProfilingEventSerializer<InstanceSummaryProfilingEvent> {
	}

	private static class SingleInstanceProfilingEventSerializer extends InstanceProfilingEventSerializer<SingleInstanceProfilingEvent> {

		@Override
		protected void writeFields(SingleInstanceProfilingEvent profilingEvent) {
			super.writeFields(profilingEvent);
			writeField("instanceName", profilingEvent.getInstanceName());
		}
	}

}
