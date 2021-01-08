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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.java.tuple.Tuple2;

import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;

public class KeyGroupRangeOffsetTest {

    @Test
    public void testKeyGroupIntersection() {
        long[] offsets = new long[9];
        for (int i = 0; i < offsets.length; ++i) {
            offsets[i] = i;
        }

        int startKeyGroup = 2;

        KeyGroupRangeOffsets keyGroupRangeOffsets =
                new KeyGroupRangeOffsets(KeyGroupRange.of(startKeyGroup, 10), offsets);
        KeyGroupRangeOffsets intersection =
                keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(3, 7));
        KeyGroupRangeOffsets expected =
                new KeyGroupRangeOffsets(
                        KeyGroupRange.of(3, 7),
                        Arrays.copyOfRange(offsets, 3 - startKeyGroup, 8 - startKeyGroup));
        Assertions.assertEquals(expected, intersection);

        Assertions.assertEquals(
                keyGroupRangeOffsets,
                keyGroupRangeOffsets.getIntersection(keyGroupRangeOffsets.getKeyGroupRange()));

        intersection = keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(11, 13));
        Assertions.assertEquals(
                KeyGroupRange.EMPTY_KEY_GROUP_RANGE, intersection.getKeyGroupRange());
        Assertions.assertFalse(intersection.iterator().hasNext());

        intersection = keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(5, 13));
        expected =
                new KeyGroupRangeOffsets(
                        KeyGroupRange.of(5, 10),
                        Arrays.copyOfRange(offsets, 5 - startKeyGroup, 11 - startKeyGroup));
        Assertions.assertEquals(expected, intersection);

        intersection = keyGroupRangeOffsets.getIntersection(KeyGroupRange.of(0, 2));
        expected =
                new KeyGroupRangeOffsets(
                        KeyGroupRange.of(2, 2),
                        Arrays.copyOfRange(offsets, 2 - startKeyGroup, 3 - startKeyGroup));
        Assertions.assertEquals(intersection, intersection);
    }

    @Test
    public void testKeyGroupRangeOffsetsBasics() {
        testKeyGroupRangeOffsetsBasicsInternal(0, 0);
        testKeyGroupRangeOffsetsBasicsInternal(0, 1);
        testKeyGroupRangeOffsetsBasicsInternal(1, 2);
        testKeyGroupRangeOffsetsBasicsInternal(42, 42);
        testKeyGroupRangeOffsetsBasicsInternal(3, 7);
        testKeyGroupRangeOffsetsBasicsInternal(0, Short.MAX_VALUE);
        testKeyGroupRangeOffsetsBasicsInternal(Short.MAX_VALUE - 1, Short.MAX_VALUE);

        try {
            testKeyGroupRangeOffsetsBasicsInternal(-3, 2);
            Assertions.fail();
        } catch (IllegalArgumentException ex) {
            // expected
        }

        KeyGroupRangeOffsets testNoGivenOffsets = new KeyGroupRangeOffsets(3, 7);
        for (int i = 3; i <= 7; ++i) {
            testNoGivenOffsets.setKeyGroupOffset(i, i + 1);
        }
        for (int i = 3; i <= 7; ++i) {
            Assertions.assertEquals(i + 1, testNoGivenOffsets.getKeyGroupOffset(i));
        }
    }

    private void testKeyGroupRangeOffsetsBasicsInternal(int startKeyGroup, int endKeyGroup) {

        long[] offsets = new long[endKeyGroup - startKeyGroup + 1];
        for (int i = 0; i < offsets.length; ++i) {
            offsets[i] = i;
        }

        KeyGroupRangeOffsets keyGroupRange =
                new KeyGroupRangeOffsets(startKeyGroup, endKeyGroup, offsets);
        KeyGroupRangeOffsets sameButDifferentConstr =
                new KeyGroupRangeOffsets(KeyGroupRange.of(startKeyGroup, endKeyGroup), offsets);
        Assertions.assertEquals(keyGroupRange, sameButDifferentConstr);

        int numberOfKeyGroup = keyGroupRange.getKeyGroupRange().getNumberOfKeyGroups();
        Assertions.assertEquals(Math.max(0, endKeyGroup - startKeyGroup + 1), numberOfKeyGroup);
        if (numberOfKeyGroup > 0) {
            Assertions.assertEquals(
                    startKeyGroup, keyGroupRange.getKeyGroupRange().getStartKeyGroup());
            Assertions.assertEquals(endKeyGroup, keyGroupRange.getKeyGroupRange().getEndKeyGroup());
            int c = startKeyGroup;
            for (Tuple2<Integer, Long> tuple : keyGroupRange) {
                Assertions.assertEquals(c, (int) tuple.f0);
                Assertions.assertTrue(keyGroupRange.getKeyGroupRange().contains(tuple.f0));
                Assertions.assertEquals((long) c - startKeyGroup, (long) tuple.f1);
                ++c;
            }

            for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
                Assertions.assertEquals(i - startKeyGroup, keyGroupRange.getKeyGroupOffset(i));
            }

            int newOffset = 42;
            for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
                keyGroupRange.setKeyGroupOffset(i, newOffset);
                ++newOffset;
            }

            for (int i = startKeyGroup; i <= endKeyGroup; ++i) {
                Assertions.assertEquals(42 + i - startKeyGroup, keyGroupRange.getKeyGroupOffset(i));
            }

            Assertions.assertEquals(endKeyGroup + 1, c);
            Assertions.assertFalse(keyGroupRange.getKeyGroupRange().contains(startKeyGroup - 1));
            Assertions.assertFalse(keyGroupRange.getKeyGroupRange().contains(endKeyGroup + 1));
        } else {
            Assertions.assertEquals(KeyGroupRange.EMPTY_KEY_GROUP_RANGE, keyGroupRange);
        }
    }
}
