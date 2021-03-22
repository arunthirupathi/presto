/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import com.facebook.presto.common.block.VariableWidthBlock;
import com.facebook.presto.orc.writer.DictionaryBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.wrappedBuffer;
import static org.testng.Assert.assertEquals;

public class TestDictionaryBuilder
{
    @Test
    public void testBlockWithHashCollision()
    {
        Slice slice1 = wrappedBuffer((byte) 1);
        Slice slice2 = wrappedBuffer((byte) 2);
        DictionaryBuilder dictionaryBuilder = new DictionaryBuilder(64);
        for (int i = 0; i < 64; i++) {
            int dictionaryIndex = dictionaryBuilder.putIfAbsent(new TestHashCollisionBlock(1, slice1, new int[] {0, 1}, new boolean[] {false}), 0);
            assertEquals(dictionaryIndex, 0);
            dictionaryIndex = dictionaryBuilder.putIfAbsent(new TestHashCollisionBlock(1, slice2, new int[] {0, 1}, new boolean[] {false}), 0);
            assertEquals(dictionaryIndex, 1);
        }
        assertEquals(dictionaryBuilder.getEntryCount(), 2);

        validatePosition(dictionaryBuilder, 0, 0, slice1);
        validatePosition(dictionaryBuilder, 1, 1, slice2);
    }

    private void validatePosition(DictionaryBuilder dictionaryBuilder, int position, int expectedOffset, Slice expectedSlice)
    {
        int length = expectedSlice.length();
        assertEquals(dictionaryBuilder.getRawSliceOffset(position), expectedOffset);
        assertEquals(dictionaryBuilder.getSliceLength(position), length);
        assertEquals(dictionaryBuilder.getSlice(position, length), expectedSlice);
        Slice rawSlice = dictionaryBuilder.getRawSlice(position);
        assertEquals(Slices.copyOf(rawSlice, expectedOffset, length), expectedSlice);
    }

    private static class TestHashCollisionBlock
            extends VariableWidthBlock
    {
        public TestHashCollisionBlock(int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
        {
            super(positionCount, slice, offsets, Optional.of(valueIsNull));
        }

        @Override
        public long hash(int position, int offset, int length)
        {
            // return same hash to generate hash collision
            return 0;
        }
    }
}
