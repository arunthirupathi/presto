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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.MethodHandleUtil;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.base.Preconditions;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.lang.reflect.Field;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class DwrfChecksum
{
    public static class PrestoTypeConverter
    {
        private PrestoTypeConverter()
        {
        }

        public static Type toPrestoType(List<OrcType> types, int index)
        {
            Preconditions.checkArgument(index < types.size());
            OrcType type = types.get(index);
            switch (type.getOrcTypeKind()) {
                case BOOLEAN:
                    return BooleanType.BOOLEAN;
                case BYTE:
                    return TinyintType.TINYINT;
                case SHORT:
                    return SmallintType.SMALLINT;
                case INT:
                    return IntegerType.INTEGER;
                case LONG:
                    return BigintType.BIGINT;
                case FLOAT:
                    return RealType.REAL;
                case DOUBLE:
                    return DoubleType.DOUBLE;
                case TIMESTAMP:
                    return TimestampType.TIMESTAMP;
                case STRING:
                    return VarcharType.createUnboundedVarcharType();
                case BINARY:
                    return VarbinaryType.VARBINARY;
                case LIST:
                    return toListType(types, index);
                case MAP:
                    return toMapType(types, index);
                case STRUCT:
                    return toRowType(types, index);
            }
            throw new RuntimeException("Unrecognized type " + type);
        }

        private static RowType toRowType(List<OrcType> types, int index)
        {
            OrcType type = types.get(index);
            Preconditions.checkArgument(type.getOrcTypeKind().equals(OrcType.OrcTypeKind.STRUCT));

            List<RowType.Field> subTypes = new ArrayList<>(type.getFieldTypeIndexes().size());
            for (int i = 0; i < type.getFieldTypeIndexes().size(); i++) {
                Type subType = toPrestoType(types, type.getFieldTypeIndexes().get(i));
                subTypes.add(new RowType.Field(Optional.of(type.getFieldNames().get(i)), subType));
            }
            return RowType.from(subTypes);
        }

        private static ArrayType toListType(List<OrcType> types, int index)
        {
            OrcType type = types.get(index);
            Preconditions.checkArgument(type.getOrcTypeKind().equals(OrcType.OrcTypeKind.LIST));
            Preconditions.checkArgument(type.getFieldTypeIndexes().size() == 1, type.getFieldTypeIndexes().toString());
            Type subType = toPrestoType(types, type.getFieldTypeIndexes().get(0));
            return new ArrayType(subType);
        }

        private static MapType toMapType(List<OrcType> types, int index)
        {
            OrcType type = types.get(index);
            Preconditions.checkArgument(type.getOrcTypeKind().equals(OrcType.OrcTypeKind.MAP));
            Preconditions.checkArgument(type.getFieldTypeIndexes().size() == 2, type.getFieldTypeIndexes().toString());
            Type keyType = toPrestoType(types, type.getFieldTypeIndexes().get(0));
            Type valueType = toPrestoType(types, type.getFieldTypeIndexes().get(1));
            return new MapType(keyType, valueType, MethodHandleUtil.methodHandle(DwrfChecksum.class, "throwUnsupportedOperation"),
                    MethodHandleUtil.methodHandle(DwrfChecksum.class, "throwUnsupportedOperation"));
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        if (args.length < 1 || args[0] == null) {
            throw new RuntimeException("Specify a valid args[0]");
        }
        if (!(new File(args[0]).exists())) {
            throw new RuntimeException("valid file name expected " + args[0]);
        }
        long result = new DwrfChecksum(args[0], 1_000).compute();
        System.out.println("Presto checksum is " + result);
    }

    private final OrcReader reader;
    private final int batchSize;

    public DwrfChecksum(String filePath, int batchSize)
            throws Exception
    {
        this.batchSize = batchSize;
        final DataSize maxMergeDistance = DataSize.valueOf("1MB");
        final DataSize maxReadSize = DataSize.valueOf("8MB");
        // make block large enough to fit the rows asked for
        final DataSize maxBlockSize = DataSize.valueOf("1GB");
        final DataSize tinyStripThresholdSize = DataSize.valueOf("1MB");

        final OrcDataSource dataSource = new FileOrcDataSource(new File(filePath), maxMergeDistance,
                maxReadSize, new DataSize(1, MEGABYTE), true);

        final OrcReaderOptions readerOptions = new OrcReaderOptions(maxMergeDistance, tinyStripThresholdSize, maxBlockSize, false);
        this.reader = new OrcReader(
                dataSource,
                OrcEncoding.DWRF,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                readerOptions,
                false,
                new DwrfEncryptionProvider(new UnsupportedEncryptionLibrary(), new TestingEncryptionLibrary()),
                DwrfKeyProvider.EMPTY);
    }

    private long compute()
            throws Exception
    {
        if (reader.getFooter().getNumberOfRows() == 0) {
            return 0;
        }

        List<OrcType> types = reader.getFooter().getTypes();
        Type convertedType = PrestoTypeConverter.toPrestoType(types, 0);
        Map<Integer, Type> includedColumns = new LinkedHashMap<>();
        RowType rowType = (RowType) convertedType;
        for (int i = 0; i < rowType.getFields().size(); i++) {
            includedColumns.put(i, rowType.getFields().get(i).getType());
        }

        try (OrcBatchRecordReader recordReader = reader.createBatchRecordReader(includedColumns, OrcPredicate.TRUE,
                DateTimeZone.forID("America/Los_Angeles"),
                NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                this.batchSize)) {
            long result = 0;
            while (true) {
                overrideNextBatchSize(recordReader, this.batchSize);
                int size = recordReader.nextBatch();
                if (size <= 0) {
                    break;
                }

                long elementSum = 0;
                for (int i = 0; i < rowType.getFields().size(); i++) {
                    Type type = rowType.getFields().get(i).getType();
                    Block block = recordReader.readBlock(i);
                    checkState(block.getPositionCount() == size);
                    elementSum = elementSum * 31 + checksum(block, type, null);
                }

                long checksum = 0;
                for (int i = 0; i < size; ++i) {
                    checksum = checksum * 31 + i;
                }
                result = result * 31 + (elementSum * 31 + checksum);
            }
            return result;
        }
    }

    private static long checksum(Block block, Type type, boolean[] nulls)
    {
        long checksum = 0;
        int size = (nulls != null ? nulls.length : block.getPositionCount());
        int blockPos = 0;
        if (BooleanType.BOOLEAN.equals(type)) {
            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (block.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        checksum += type.getBoolean(block, blockPos) ? 1 : 0;
                    }
                    ++blockPos;
                }
            }
            return checksum;
        }
        if (BigintType.BIGINT.equals(type) || IntegerType.INTEGER.equals(type)
                || SmallintType.SMALLINT.equals(type) || TinyintType.TINYINT.equals(type)) {
            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (block.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        checksum += type.getLong(block, blockPos);
                    }
                    ++blockPos;
                }
            }
            return checksum;
        }
        if (RealType.REAL.equals(type)) {
            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (block.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        int num = (int) type.getLong(block, blockPos);
                        int checksumVal = Float.floatToIntBits(Float.intBitsToFloat(num));
                        checksum += checksumVal;
                    }
                    ++blockPos;
                }
            }
            return checksum;
        }
        if (DoubleType.DOUBLE.equals(type)) {
            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (block.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        double value = type.getDouble(block, blockPos);
                        checksum += Double.doubleToLongBits(value);
                    }
                    ++blockPos;
                }
            }
            return checksum;
        }
        if (type instanceof VarcharType || VarbinaryType.VARBINARY.equals(type)) {
            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (block.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        long strChecksum = 0;
                        byte[] bytes = type.getSlice(block, blockPos).getBytes();
                        for (byte aByte : bytes) {
                            strChecksum = strChecksum * 31 + aByte;
                        }
                        checksum += strChecksum;
                    }
                    ++blockPos;
                }
            }
            return checksum;
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (block.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        // Presto truncates timestamp to millis precision inside the reader.
                        long millis = type.getLong(block, blockPos);
                        Timestamp timestamp = new Timestamp(millis);
                        long sec = timestamp.getTime() / 1000;
                        long nanos = timestamp.getNanos();
                        if (nanos != 0 && millis < 0) {
                            sec--;
                        }

                        checksum += sec;
                        checksum *= 31;
                        checksum += nanos;
                    }
                    ++blockPos;
                }
            }
            return checksum;
        }
        else if (isArrayType(type)) {
            Type childType = type.getTypeParameters().get(0);
            ColumnarArray columnarArray = ColumnarArray.toColumnarArray(block);
            long listChecksum = checksum(columnarArray.getElementsBlock(), childType, null);

            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (columnarArray.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        checksum += columnarArray.getLength(blockPos);
                    }
                    ++blockPos;
                }
            }
            return listChecksum * 31 + checksum;
        }
        else if (isMapType(type)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            ColumnarMap columnarMap = ColumnarMap.toColumnarMap(block);
            Block keyBlock = columnarMap.getKeysBlock();
            Block valueBlock = columnarMap.getValuesBlock();

            long keyChecksum = checksum(keyBlock, keyType, null);
            long valueChecksum = checksum(valueBlock, valueType, null);

            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    checksum += pos;
                }
                else {
                    if (columnarMap.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        checksum += columnarMap.getEntryCount(blockPos);
                    }
                    ++blockPos;
                }
            }
            return (keyChecksum * 31 + valueChecksum) * 31 + checksum;
        }
        else if (isRowType(type)) {
            ColumnarRow columnarRow = ColumnarRow.toColumnarRow(block);
            List<Type> fieldTypes = type.getTypeParameters();
            long elementSum = 0;
            boolean[] childNulls = new boolean[size];
            boolean hasNulls = false;
            for (int pos = 0; pos < size; pos++) {
                checksum *= 31;
                if (nulls != null && nulls[pos]) {
                    childNulls[pos] = true;
                    hasNulls = true;
                }
                else {
                    if (!columnarRow.isNull(blockPos)) {
                        checksum += pos;
                    }
                    else {
                        childNulls[pos] = true;
                        hasNulls = true;
                    }
                    ++blockPos;
                }
            }

            for (int i = 0; i < fieldTypes.size(); i++) {
                Block childBlock = columnarRow.getField(i);
                elementSum = elementSum * 31 +
                        checksum(childBlock, fieldTypes.get(i), hasNulls ? childNulls : null);
            }
            return elementSum * 31 + checksum;
        }
        throw new UnsupportedOperationException("Unsupported type " + type.getClass().getSimpleName());
    }

    private static boolean isArrayType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ARRAY);
    }

    private static boolean isMapType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.MAP);
    }

    private static boolean isRowType(Type type)
    {
        return type.getTypeSignature().getBase().equals(StandardTypes.ROW);
    }

    private static void overrideField(OrcBatchRecordReader recordReader, String fieldName, int batchSize)
            throws Exception
    {
        Field field = recordReader.getClass().getSuperclass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setInt(recordReader, batchSize);
    }

    private static void overrideNextBatchSize(OrcBatchRecordReader recordReader, int batchSize)
            throws Exception
    {
        // Presto supports only specifying initial batch size and does
        // adaptive batching. But current checksum logic, expects
        // batch size should be Min(remainingRowsInStripe, BatchSize)

        // Hacking these 2 fields to batch size achieves that currently
        // But it could break any time when the Presto changes logic.
        // Basic test exists to verify the presence of these private fields
        overrideField(recordReader, "nextBatchSize", batchSize);
        overrideField(recordReader, "maxBatchSize", batchSize);
    }

    // This method is invoked via reflection
    public static void throwUnsupportedOperation()
    {
        throw new UnsupportedOperationException();
    }
}
