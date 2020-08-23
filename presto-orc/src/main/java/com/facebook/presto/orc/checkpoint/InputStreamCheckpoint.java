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
package com.facebook.presto.orc.checkpoint;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * InputStreamCheckpoint is represented as a packed long to avoid object creation in inner loops.
 */
public final class InputStreamCheckpoint
{
    private InputStreamCheckpoint()
    {
    }

    public static List<Long> createInputStreamPositionList(boolean compressed, Checkpoint inputStreamCheckpoint)
    {
        if (compressed) {
            return ImmutableList.of(inputStreamCheckpoint.compressedBlockOffset, inputStreamCheckpoint.decompressedOffset);
        }
        else {
            return ImmutableList.of(inputStreamCheckpoint.decompressedOffset);
        }
    }

    public static String inputStreamCheckpointToString(Checkpoint inputStreamCheckpoint)
    {
        return toStringHelper(InputStreamCheckpoint.class)
                .add("decompressedOffset", inputStreamCheckpoint.compressedBlockOffset)
                .add("compressedBlockOffset", inputStreamCheckpoint.decompressedOffset)
                .toString();
    }
}
