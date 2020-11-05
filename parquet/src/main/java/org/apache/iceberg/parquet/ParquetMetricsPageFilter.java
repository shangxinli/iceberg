/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.parquet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Binder;
import org.apache.iceberg.expressions.BoundReference;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.ExpressionVisitors.BoundExpressionVisitor;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.BinaryUtil;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.ColumnIndex;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetMetricsPageFilter {
  private final Schema schema;
  private final Expression expr;

  public ParquetMetricsPageFilter(Schema schema, Expression unbound) {
    this(schema, unbound, true);
  }

  public ParquetMetricsPageFilter(Schema schema, Expression unbound, boolean caseSensitive) {
    this.schema = schema;
    StructType struct = schema.asStruct();
    this.expr = Binder.bind(struct, Expressions.rewriteNot(unbound), caseSensitive);
  }

  /**
   * Test whether the file may contain records that match the expression.
   *
   * @param fileSchema schema for the Parquet file
   * @param rowGroup metadata for a row group
   * @return false if the file cannot contain rows that match the expression, true otherwise.
   */
  public boolean shouldRead(MessageType fileSchema, BlockMetaData rowGroup, ParquetFileReader reader) {
    return new MetricsEvalVisitor().eval(fileSchema, rowGroup, reader);
  }

  private static final boolean ROWS_MIGHT_MATCH = true;
  private static final boolean ROWS_CANNOT_MATCH = false;

  private class MetricsEvalVisitor extends BoundExpressionVisitor<Boolean> {
    private Map<Integer, Statistics> stats = null;
    private Map<Integer, Long> valueCounts = null;
    private Map<Integer, Function<Object, Object>> conversions = null;
    private Map<Integer, ColumnIndex> columnIndexes = null;

    private boolean eval(MessageType fileSchema, BlockMetaData rowGroup, ParquetFileReader reader) {
      if (rowGroup.getRowCount() <= 0) {
        return ROWS_CANNOT_MATCH;
      }

      this.stats = Maps.newHashMap();
      this.valueCounts = Maps.newHashMap();
      this.conversions = Maps.newHashMap();
      this.columnIndexes = Maps.newHashMap();
      for (ColumnChunkMetaData col : rowGroup.getColumns()) {
        PrimitiveType colType = fileSchema.getType(col.getPath().toArray()).asPrimitiveType();
        if (colType.getId() != null) {
          int id = colType.getId().intValue();
          // stats.put(id, col.getStatistics());
          valueCounts.put(id, col.getValueCount());
          conversions.put(id, ParquetConversions.converterFromParquet(colType));
          try {
            ColumnIndex columnIndex = reader.readColumnIndex(col);
            // Null of columnIndex is valid
            Preconditions.checkState(columnIndex == null ||
                            (columnIndex.getMinValues().size() == columnIndex.getMaxValues().size() &&
                                    columnIndex.getMaxValues().size() == columnIndex.getNullPages().size() &&
                                    columnIndex.getNullPages().size() == columnIndex.getNullCounts().size()),
                    "The ColumnIndex values are corrupted for column %s", col.getPath());
            columnIndexes.put(id, columnIndex);
          } catch (IOException e) {
            throw new RuntimeIOException(e);
          }
        }
      }

      return ExpressionVisitors.visitEvaluator(expr, this);
    }

    @Override
    public Boolean alwaysTrue() {
      return ROWS_MIGHT_MATCH; // all rows match
    }

    @Override
    public Boolean alwaysFalse() {
      return ROWS_CANNOT_MATCH; // all rows fail
    }

    @Override
    public Boolean not(Boolean result) {
      return !result;
    }

    @Override
    public Boolean and(Boolean leftResult, Boolean rightResult) {
      return leftResult && rightResult;
    }

    @Override
    public Boolean or(Boolean leftResult, Boolean rightResult) {
      return leftResult || rightResult;
    }

    @Override
    public <T> Boolean isNull(BoundReference<T> ref) {
      // no need to check whether the field is required because binding evaluates that case
      // if the column has no null values, the expression cannot match
      Integer id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      ColumnIndex columnIndex = columnIndexes.get(id);
      if (valueCount == null || columnIndex == null) {
        // the column is not present and is all nulls
        return ROWS_MIGHT_MATCH;
      }

      // Should we convert to map for contains to optimize
      if (!hasNullPages(columnIndex) && !hasNullCounts(columnIndex)) {
        return ROWS_CANNOT_MATCH;
      }

      return ROWS_MIGHT_MATCH;
    }

    private boolean hasNullCounts(ColumnIndex columnIndex) {
      for (long nullCount : columnIndex.getNullCounts()) {
        if (nullCount > 0) {
          return true;
        }
      }
      return false;
    }

    private boolean hasNullPages(ColumnIndex columnIndex) {
      for (boolean nullPage : columnIndex.getNullPages()) {
        if (nullPage) {
          return true;
        }
      }
      return false;
    }

    @Override
    public <T> Boolean notNull(BoundReference<T> ref) {
      // no need to check whether the field is required because binding evaluates that case
      // if the column has no non-null values, the expression cannot match
      Integer id = ref.fieldId();

      // When filtering nested types notNull() is implicit filter passed even though complex
      // filters aren't pushed down in Parquet. Leave all nested column type filters to be
      // evaluated post scan.
      if (schema.findType(id) instanceof Type.NestedType) {
        return ROWS_MIGHT_MATCH;
      }

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      ColumnIndex columnIndex = columnIndexes.get(id);
      if (columnIndex == null) {
        // no way to determine
        return ROWS_MIGHT_MATCH;
      }

      if (!hasNullPages(columnIndex) && !hasNullCounts(columnIndex)) {
        return ROWS_MIGHT_MATCH;
      }

      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean lt(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (!colStats.hasNonNullValue()) {
          return ROWS_CANNOT_MATCH;
        }

        T lower = min(colStats, id);
        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp >= 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean ltEq(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (!colStats.hasNonNullValue()) {
          return ROWS_CANNOT_MATCH;
        }

        T lower = min(colStats, id);
        int cmp = lit.comparator().compare(lower, lit.value());
        if (cmp > 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gt(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (!colStats.hasNonNullValue()) {
          return ROWS_CANNOT_MATCH;
        }

        T upper = max(colStats, id);
        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp <= 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean gtEq(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (!colStats.hasNonNullValue()) {
          return ROWS_CANNOT_MATCH;
        }

        T upper = max(colStats, id);
        int cmp = lit.comparator().compare(upper, lit.value());
        if (cmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean eq(BoundReference<T> ref, Literal<T> lit) {
      Integer id = ref.fieldId();

      // When filtering nested types notNull() is implicit filter passed even though complex
      // filters aren't pushed down in Parquet. Leave all nested column type filters to be
      // evaluated post scan.
      if (schema.findType(id) instanceof Type.NestedType) {
        return ROWS_MIGHT_MATCH;
      }

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      ColumnIndex columnIndex = columnIndexes.get(id);
      if (columnIndex == null) {
        // no way to determine
        return ROWS_MIGHT_MATCH;
      }

      return inRange(id, columnIndex, lit);
    }

    private <T> Boolean inRange(Integer id, ColumnIndex columnIndex, Literal<T> lit) {
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      for (int i = 0; i < maxValues.size(); i++) {
        T min = convert(minValues.get(i), id);
        int cmpMin = lit.comparator().compare(min, lit.value());
        T max = convert(maxValues.get(i), id);
        int cmpMax = lit.comparator().compare(max, lit.value());
        if (cmpMin <= 0 && cmpMax >= 0) {
          return ROWS_MIGHT_MATCH;
        }
      }
      return ROWS_CANNOT_MATCH;
    }

    @Override
    public <T> Boolean notEq(BoundReference<T> ref, Literal<T> lit) {
      // TODO: Do we have notEQ in columnIndex readNextFilterRowGroup? If yes, we need them here.
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notEq(col, X) with (X, Y) doesn't guarantee that X is a value in col.
      return ROWS_MIGHT_MATCH;
    }

    @Override
    public <T> Boolean in(BoundReference<T> ref, Set<T> literalSet) {
      Integer id = ref.fieldId();

      // When filtering nested types notNull() is implicit filter passed even though complex
      // filters aren't pushed down in Parquet. Leave all nested column type filters to be
      // evaluated post scan.
      if (schema.findType(id) instanceof Type.NestedType) {
        return ROWS_MIGHT_MATCH;
      }

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      ColumnIndex columnIndex = columnIndexes.get(id);
      if (columnIndex == null) {
        // no way to determine
        return ROWS_MIGHT_MATCH;
      }

      // all null cannot match
      Statistics<?> colStats = stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (!colStats.hasNonNullValue()) {
          return ROWS_CANNOT_MATCH;
        }

        Collection<T> literals = literalSet;

        T lower = min(colStats, id);
        literals = literals.stream().filter(v -> ref.comparator().compare(lower, v) <= 0).collect(Collectors.toList());
        if (literals.isEmpty()) {  // if all values are less than lower bound, rows cannot match.
          return ROWS_CANNOT_MATCH;
        }

        T upper = max(colStats, id);
        literals = literals.stream().filter(v -> ref.comparator().compare(upper, v) >= 0).collect(Collectors.toList());
        if (literals.isEmpty()) { // if all remaining values are greater than upper bound, rows cannot match.
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    private <T> Boolean inRangeForIn(Integer id, ColumnIndex columnIndex, Literal<T> lit) {
      List<ByteBuffer> minValues = columnIndex.getMinValues();
      List<ByteBuffer> maxValues = columnIndex.getMaxValues();
      for (int i = 0; i < maxValues.size(); i++) {
        T min = convert(minValues.get(i), id);
        int cmpMin = lit.comparator().compare(min, lit.value());
        T max = convert(maxValues.get(i), id);
        int cmpMax = lit.comparator().compare(max, lit.value());
        if (cmpMin <= 0 && cmpMax >= 0) {
          return ROWS_MIGHT_MATCH;
        }
      }
      return ROWS_CANNOT_MATCH;
    }


    @Override
    public <T> Boolean notIn(BoundReference<T> ref, Set<T> literalSet) {
      // because the bounds are not necessarily a min or max value, this cannot be answered using
      // them. notIn(col, {X, ...}) with (X, Y) doesn't guarantee that X is a value in col.
      return ROWS_MIGHT_MATCH;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Boolean startsWith(BoundReference<T> ref, Literal<T> lit) {
      int id = ref.fieldId();

      Long valueCount = valueCounts.get(id);
      if (valueCount == null) {
        // the column is not present and is all nulls
        return ROWS_CANNOT_MATCH;
      }

      Statistics<Binary> colStats = (Statistics<Binary>) stats.get(id);
      if (colStats != null && !colStats.isEmpty()) {
        if (!colStats.hasNonNullValue()) {
          return ROWS_CANNOT_MATCH;
        }

        ByteBuffer prefixAsBytes = lit.toByteBuffer();

        Comparator<ByteBuffer> comparator = Comparators.unsignedBytes();

        Binary lower = colStats.genericGetMin();
        // truncate lower bound so that its length in bytes is not greater than the length of prefix
        int lowerLength = Math.min(prefixAsBytes.remaining(), lower.length());
        int lowerCmp = comparator.compare(BinaryUtil.truncateBinary(lower.toByteBuffer(), lowerLength), prefixAsBytes);
        if (lowerCmp > 0) {
          return ROWS_CANNOT_MATCH;
        }

        Binary upper = colStats.genericGetMax();
        // truncate upper bound so that its length in bytes is not greater than the length of prefix
        int upperLength = Math.min(prefixAsBytes.remaining(), upper.length());
        int upperCmp = comparator.compare(BinaryUtil.truncateBinary(upper.toByteBuffer(), upperLength), prefixAsBytes);
        if (upperCmp < 0) {
          return ROWS_CANNOT_MATCH;
        }
      }

      return ROWS_MIGHT_MATCH;
    }

    @SuppressWarnings("unchecked")
    private <T> T min(Statistics<?> statistics, int id) {
      return (T) conversions.get(id).apply(statistics.genericGetMin());
    }

    private <T> T convert(ByteBuffer buf, int id) {
      return (T) conversions.get(id).apply(buf);
    }

    @SuppressWarnings("unchecked")
    private <T> T max(Statistics<?> statistics, int id) {
      return (T) conversions.get(id).apply(statistics.genericGetMax());
    }
  }
}
