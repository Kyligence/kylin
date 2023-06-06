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

package org.apache.kylin.metadata.model.tool;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;

import org.apache.calcite.sql.SqlKind;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.metadata.model.NonEquiJoinCondition;
import org.apache.kylin.metadata.model.NonEquiJoinConditionType;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NonEquiJoinConditionComparator {

    private static final Map<SqlKind, TruthTable.Operator> OP_TRUTH_MAPPING = ImmutableMap.<SqlKind, TruthTable.Operator> builder()
            .put(SqlKind.AND, TruthTable.Operator.AND) //
            .put(SqlKind.OR, TruthTable.Operator.OR) //
            .put(SqlKind.NOT, TruthTable.Operator.NOT) //
            .build();

    private static final Map<SqlKind, SqlKind> OP_INVERSE_MAPPING = ImmutableMap.<SqlKind, SqlKind> builder()
            .put(SqlKind.NOT_EQUALS, SqlKind.EQUALS) //
            .put(SqlKind.NOT_IN, SqlKind.IN) //
            .put(SqlKind.IS_NOT_NULL, SqlKind.IS_NULL) //
            .put(SqlKind.IS_NOT_FALSE, SqlKind.IS_FALSE) //
            .put(SqlKind.IS_NOT_TRUE, SqlKind.IS_TRUE) //
            .put(SqlKind.IS_NOT_DISTINCT_FROM, SqlKind.IS_DISTINCT_FROM) //
            .put(SqlKind.LESS_THAN, SqlKind.GREATER_THAN_OR_EQUAL) //
            .put(SqlKind.LESS_THAN_OR_EQUAL, SqlKind.GREATER_THAN) //
            .build();

    private NonEquiJoinConditionComparator() {
    }

    public static boolean equals(NonEquiJoinCondition cond1, NonEquiJoinCondition cond2) {
        try {
            return TruthTable.equals(createTruthTable(cond1), createTruthTable(cond2));
        } catch (Throwable e) {
            log.error("Error on compareing cond1 {}, cond2 {}", cond1, cond2, e);
            return false;
        }
    }

    private static TruthTable createTruthTable(NonEquiJoinCondition nonEquiJoinCondition) {
        TruthTable.TruthTableBuilder<NonEquiJoinCondition> builder = new TruthTable.TruthTableBuilder<>(
                new NonEquiJoinConditionOperandComparator());
        buildExpr(nonEquiJoinCondition, builder);
        return builder.build();
    }

    private static void buildExpr(NonEquiJoinCondition nonEquiJoinCondition,
            TruthTable.TruthTableBuilder<NonEquiJoinCondition> builder) {
        switch (nonEquiJoinCondition.getOp()) {
        case AND:
        case NOT:
        case OR:
            builder.compositeStart(OP_TRUTH_MAPPING.get(nonEquiJoinCondition.getOp()));
            for (int i = 0; i < nonEquiJoinCondition.getOperands().length; i++) {
                buildExpr(nonEquiJoinCondition.getOperands()[i], builder);
            }
            builder.compositeEnd();
            break;
        default:
            addOperand(nonEquiJoinCondition, builder);
            break;
        }
    }

    private static void addOperand(NonEquiJoinCondition nonEquiJoinCondition,
            TruthTable.TruthTableBuilder<NonEquiJoinCondition> builder) {
        NonEquiJoinCondition normalized = nonEquiJoinCondition.copy();
        if (inverseCondOperator(normalized)) {
            builder.compositeStart(TruthTable.Operator.NOT);
            normalizedCondOperandOrderings(normalized);
            builder.addOperand(normalized);
            builder.compositeEnd();
        } else {
            normalizedCondOperandOrderings(normalized);
            builder.addOperand(normalized);
        }
    }

    private static boolean inverseCondOperator(NonEquiJoinCondition cond) {
        if (OP_INVERSE_MAPPING.containsKey(cond.getOp())) {
            cond.setOp(OP_INVERSE_MAPPING.get(cond.getOp()));
            cond.setOpName(cond.getOp().sql);
            return true;
        }
        return false;
    }

    private static void normalizedCondOperandOrderings(NonEquiJoinCondition nonEquiJoinCondition) {
        // if operands ordering does not matter, sort by operands' digest
        // case =, <>
        if (nonEquiJoinCondition.getOp() == SqlKind.EQUALS) {
            Arrays.sort(nonEquiJoinCondition.getOperands(), Comparator.comparing(NonEquiJoinCondition::toString));
        }

        // sort IN args
        if (nonEquiJoinCondition.getOp() == SqlKind.IN) {
            Arrays.sort(nonEquiJoinCondition.getOperands(), 1, nonEquiJoinCondition.getOperands().length,
                    Comparator.comparing(NonEquiJoinCondition::toString));
        }
    }

    public static class NonEquiJoinConditionOperandComparator implements Comparator<NonEquiJoinCondition> {

        @Override
        public int compare(NonEquiJoinCondition cond1, NonEquiJoinCondition cond2) {
            if (!(Objects.equals(cond1.getOp(), cond2.getOp())
                    && cond1.getOperands().length == cond2.getOperands().length
                    && Objects.equals(cond1.getType(), cond2.getType())
                    && Objects.equals(cond1.getDataType(), cond2.getDataType()))) {
                return 1;
            }

            // compare opName on for SqlKind OTHER
            if (cond1.getOp() == SqlKind.OTHER || cond1.getOp() == SqlKind.OTHER_FUNCTION) {
                if (!Objects.equals(cond1.getOpName(), cond2.getOpName())) {
                    return 1;
                }
            }

            if (cond1.getType() == NonEquiJoinConditionType.LITERAL) {
                return Objects.equals(cond1.getValue(), cond2.getValue()) ? 0 : 1;
            } else if (cond1.getType() == NonEquiJoinConditionType.COLUMN) {
                return Objects.equals(cond1.getColRef().getColumnDesc(), cond2.getColRef().getColumnDesc()) ? 0 : 1;
            } else {
                NonEquiJoinCondition[] sorted1 = cond1.getSortedOperands();
                NonEquiJoinCondition[] sorted2 = cond2.getSortedOperands();
                for (int i = 0; i < sorted1.length; i++) {
                    if (compare(sorted1[i], sorted2[i]) != 0) {
                        return 1;
                    }
                }
                return 0;
            }
        }
    }
}
