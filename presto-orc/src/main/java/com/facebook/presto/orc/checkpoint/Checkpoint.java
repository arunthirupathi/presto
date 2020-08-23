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

public class Checkpoint
{
    public final long compressedBlockOffset;
    public final long decompressedOffset;

    public Checkpoint(long compressedBlockOffset, long decompressedOffset)
    {
        this.compressedBlockOffset = compressedBlockOffset;
        this.decompressedOffset = decompressedOffset;
    }

    public static Checkpoint from(boolean isCompressed, Checkpoints.ColumnPositionsList positionsList)
    {
        return isCompressed ?
                new Checkpoint(positionsList.nextPosition(), positionsList.nextPosition()) :
                new Checkpoint(0, positionsList.nextPosition());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Checkpoint) {
            Checkpoint other = (Checkpoint) obj;
            return compressedBlockOffset == other.compressedBlockOffset && decompressedOffset == other.decompressedOffset;
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
    }
}
