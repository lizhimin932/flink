/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.streaming.api.watermark.Watermark;

import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StatusWatermarkValve}. While tests in {@link org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTest}
 * and {@link org.apache.flink.streaming.runtime.tasks.TwoInputStreamTaskTest} may also implicitly test {@link StatusWatermarkValve}
 * and that valves are correctly used in the tasks' input processors, the unit tests here additionally makes sure that
 * the watermarks and stream statuses to forward are generated from the valve at the exact correct times and in a
 * deterministic behaviour. The unit tests here also test more complex stream status / watermark input cases.
 *
 * <p>
 * The tests are performed by a series of watermark and stream status inputs to the valve. On every input method call,
 * the output is checked to contain only the expected watermark or stream status, and nothing else. This ensures that
 * no redundant outputs are generated by the output logic of {@link StatusWatermarkValve}. The behaviours that a series of
 * input calls to the valve is trying to test is explained as inline comments within the tests.
 */
public class StatusWatermarkValveTest {

	/**
	 * Tests that all input channels of a valve start as ACTIVE stream status.
	 */
	@Test
	public void testAllInputChannelsStartAsActive() {
		BufferedValveOutputHandler valveOutput = new BufferedValveOutputHandler();
		StatusWatermarkValve valve = new StatusWatermarkValve(4, valveOutput);

		// ------------------------------------------------------------------------
		//  Ensure that the valve will output an IDLE stream status as soon as
		//  all input channels become IDLE; this also implicitly ensures that
		//  all input channels start as ACTIVE.
		// ------------------------------------------------------------------------

		valve.inputStreamStatus(StreamStatus.IDLE, 3);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputStreamStatus(StreamStatus.IDLE, 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputStreamStatus(StreamStatus.IDLE, 1);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputStreamStatus(StreamStatus.IDLE, 2);
		assertEquals(StreamStatus.IDLE, valveOutput.popLastOutputStreamStatus());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());
	}

	/**
	 * Tests that valves work as expected when they handle only 1 input channel.
	 * Tested behaviours are explained as inline comments.
	 */
	@Test
	public void testOneInputValve() {
		BufferedValveOutputHandler valveOutput = new BufferedValveOutputHandler();
		StatusWatermarkValve valve = new StatusWatermarkValve(1, valveOutput);

		// start off with an ACTIVE status; since the valve should initially start as ACTIVE,
		// no state change is toggled, therefore no stream status should be emitted
		valve.inputStreamStatus(StreamStatus.ACTIVE, 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// input some monotonously increasing watermarks while ACTIVE;
		// the exact same watermarks should be emitted right after the inputs
		valve.inputWatermark(new Watermark(0), 0);
		assertEquals(new Watermark(0), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(25), 0);
		assertEquals(new Watermark(25), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// decreasing watermarks should not result in any output
		valve.inputWatermark(new Watermark(18), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(42), 0);
		assertEquals(new Watermark(42), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// toggling ACTIVE to IDLE should result in an IDLE stream status output
		valve.inputStreamStatus(StreamStatus.IDLE, 0);
		assertEquals(StreamStatus.IDLE, valveOutput.popLastOutputStreamStatus());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// watermark inputs should be ignored while all input channels (only 1 in this case) are IDLE
		valve.inputWatermark(new Watermark(52), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(60), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// no status change toggle while IDLE should result in stream status outputs
		valve.inputStreamStatus(StreamStatus.IDLE, 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// toggling IDLE to ACTIVE should result in an ACTIVE stream status output
		valve.inputStreamStatus(StreamStatus.ACTIVE, 0);
		assertEquals(StreamStatus.ACTIVE, valveOutput.popLastOutputStreamStatus());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// the valve should remember the last watermark input channels received while they were ACTIVE (which was 42);
		// decreasing watermarks should therefore still be ignored, even after a status toggle
		valve.inputWatermark(new Watermark(40), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// monotonously increasing watermarks after resuming to be ACTIVE should be output normally
		valve.inputWatermark(new Watermark(68), 0);
		assertEquals(new Watermark(68), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(72), 0);
		assertEquals(new Watermark(72), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());
	}

	/**
	 * Tests that valves work as expected when they handle multiple input channels (tested with 3).
	 * Tested behaviours are explained as inline comments.
	 */
	@Test
	public void testMultipleInputValve() {
		BufferedValveOutputHandler valveOutput = new BufferedValveOutputHandler();
		StatusWatermarkValve valve = new StatusWatermarkValve(3, valveOutput);

		// ------------------------------------------------------------------------
		//  Ensure that watermarks are output only when all
		//  channels have been input some watermark.
		// ------------------------------------------------------------------------

		valve.inputWatermark(new Watermark(0), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(0), 1);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(0), 2);
		assertEquals(new Watermark(0), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// ------------------------------------------------------------------------
		//  Ensure that watermarks are output as soon as the overall min
		//  watermark across all channels have advanced.
		// ------------------------------------------------------------------------

		valve.inputWatermark(new Watermark(12), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(8), 2);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(10), 2);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(15), 1);
		// lowest watermark across all channels is now channel 2, with watermark @ 10
		assertEquals(new Watermark(10), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// ------------------------------------------------------------------------
		//  Ensure that decreasing watermarks are ignored
		// ------------------------------------------------------------------------

		valve.inputWatermark(new Watermark(6), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// ------------------------------------------------------------------------
		//  Ensure that when some input channel becomes idle, that channel will
		//  no longer be accounted for when advancing the watermark.
		// ------------------------------------------------------------------------

		// marking channel 2 as IDLE shouldn't result in overall status toggle for the valve,
		// because there are still other active channels (0 and 1), so there should not be any
		// stream status outputs;
		// also, now that channel 2 is IDLE, the overall min watermark is 12 (from channel 0),
		// so the valve should output that
		valve.inputStreamStatus(StreamStatus.IDLE, 2);
		assertEquals(new Watermark(12), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// from now on, since channel 2 is IDLE, the valve should use watermarks only from
		// channel 0 and 1 to find the min watermark, even if channel 2 has the lowest watermark (10)
		valve.inputWatermark(new Watermark(17), 0);
		assertEquals(new Watermark(15), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(25), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(20), 1);
		assertEquals(new Watermark(20), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// ------------------------------------------------------------------------
		//  Ensure that after some channel resumes to be ACTIVE, it needs to
		//  catch up" with the current overall min watermark before it can be
		//  accounted for again when finding the min watermark across channels.
		//  Also tests that before the resumed channel catches up, the overall
		//  min watermark can still advance with watermarks of other channels.
		// ------------------------------------------------------------------------

		// resuming channel 2 to be ACTIVE shouldn't result in overall status toggle for the valve,
		// because the valve wasn't overall IDLE, so there should not be any stream status outputs;
		valve.inputStreamStatus(StreamStatus.ACTIVE, 2);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// although watermarks for channel 2 will now be accepted, it still
		// hasn't caught up with the overall min watermark (20)
		valve.inputWatermark(new Watermark(18), 2);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// since channel 2 hasn't caught up yet, it is still ignored when advancing new min watermarks
		valve.inputWatermark(new Watermark(22), 1);
		assertEquals(new Watermark(22), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(28), 0);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(33), 1);
		assertEquals(new Watermark(28), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// now, channel 2 has caught up with the overall min watermark
		valve.inputWatermark(new Watermark(30), 2);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(31), 0);
		// this acknowledges that channel 2's watermark is being accounted for again
		assertEquals(new Watermark(30), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(34), 2);
		assertEquals(new Watermark(31), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// ------------------------------------------------------------------------
		//  Ensure that once all channels are IDLE, the valve should also
		//  determine itself to be IDLE output a IDLE stream status
		// ------------------------------------------------------------------------

		valve.inputStreamStatus(StreamStatus.IDLE, 0);
		// this is because once channel 0 becomes IDLE,
		// the new min watermark will be 33 (channel 1)
		assertEquals(new Watermark(33), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputStreamStatus(StreamStatus.IDLE, 2);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputStreamStatus(StreamStatus.IDLE, 1);
		assertEquals(StreamStatus.IDLE, valveOutput.popLastOutputStreamStatus());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// ------------------------------------------------------------------------
		//  Ensure that channels gradually become ACTIVE again, the above behaviours
		//  still hold. Also ensure that as soon as one of the input channels
		//  become ACTIVE, the valve is ACTIVE again and outputs an ACTIVE stream status.
		// ------------------------------------------------------------------------

		// let channel 0 resume to be ACTIVE
		valve.inputStreamStatus(StreamStatus.ACTIVE, 0);
		assertEquals(StreamStatus.ACTIVE, valveOutput.popLastOutputStreamStatus());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// channel 0 is the only ACTIVE channel now, and is the only channel
		// accounted for when advancing min watermark
		valve.inputWatermark(new Watermark(36), 0);
		assertEquals(new Watermark(36), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// new also let channel 1 become ACTIVE
		valve.inputStreamStatus(StreamStatus.ACTIVE, 1);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// channel 1 is still behind overall min watermark
		valve.inputWatermark(new Watermark(35), 1);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// since channel 1 is still behind, channel 0 remains to be the only
		// channel used to advance min watermark
		valve.inputWatermark(new Watermark(37), 0);
		assertEquals(new Watermark(37), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		// now, channel 1 has caught up with the overall min watermark
		valve.inputWatermark(new Watermark(38), 1);
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());

		valve.inputWatermark(new Watermark(40), 0);
		// this acknowledges that channel 1's watermark is being accounted for again
		assertEquals(new Watermark(38), valveOutput.popLastOutputWatermark());
		assertTrue(valveOutput.hasNoOutputWatermarks());
		assertTrue(valveOutput.hasNoOutputStreamStatuses());
	}

	private class BufferedValveOutputHandler implements StatusWatermarkValve.ValveOutputHandler {
		private BlockingQueue<Watermark> outputWatermarks = new LinkedBlockingQueue<>();
		private BlockingQueue<StreamStatus> outputStreamStatuses = new LinkedBlockingQueue<>();

		@Override
		public void handleWatermark(Watermark watermark) {
			outputWatermarks.add(watermark);
		}

		@Override
		public void handleStreamStatus(StreamStatus streamStatus) {
			outputStreamStatuses.add(streamStatus);
		}

		public Watermark popLastOutputWatermark() {
			return outputWatermarks.poll();
		}

		public StreamStatus popLastOutputStreamStatus() {
			return outputStreamStatuses.poll();
		}

		public boolean hasNoOutputWatermarks() {
			return outputWatermarks.size() == 0;
		}

		public boolean hasNoOutputStreamStatuses() {
			return outputStreamStatuses.size() == 0;
		}
	}

}
