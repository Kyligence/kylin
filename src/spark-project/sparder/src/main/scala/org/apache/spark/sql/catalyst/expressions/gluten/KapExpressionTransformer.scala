/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.expressions.gluten

import com.google.common.collect.Lists
import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression._
import org.apache.gluten.extension.ExpressionExtensionTrait
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.{BinaryType, DataType, LongType}
import org.apache.spark.sql.udaf._

import scala.collection.mutable.ListBuffer

case class KeBitmapFunctionTransformer(
  substraitExprName: String,
  child: ExpressionTransformer,
  original: Expression) extends ExpressionTransformerWithOrigin with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    val childNode = child.doTransform(args)
    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]
    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(
        substraitExprName,
        original.children.map(_.dataType),
        ConverterUtils.FunctionConfig.OPT))

    val expressionNodes = Lists.newArrayList(childNode)
    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

case class CustomerExpressionTransformer() extends ExpressionExtensionTrait {

  /** Generate the extension expressions list, format: Sig[XXXExpression]("XXXExpressionName") */
  def expressionSigList: Seq[Sig] = Seq(
    Sig[FloorDateTime]("floor_datetime"),
    Sig[PreciseCardinality]("ke_bitmap_cardinality"),
    Sig[PreciseCountDistinctDecode]("ke_bitmap_cardinality"),
    Sig[ReusePreciseCountDistinct]("ke_bitmap_or_data"),
    Sig[PreciseCountDistinctAndValue]("ke_bitmap_and_value"),
    Sig[PreciseCountDistinctAndArray]("ke_bitmap_and_ids"),
    Sig[PreciseCountDistinct]("ke_bitmap_or_cardinality"),
    Sig[KylinTimestampAdd]("kylin_timestamp_add")
  )

  /** Replace extension expression to transformer. */
  override def replaceWithExtensionExpressionTransformer(
    substraitExprName: String,
    expr: Expression,
    attributeSeq: Seq[Attribute]): ExpressionTransformer = expr match {
    case preciseCardinality: PreciseCardinality =>
      KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCardinality.child, attributeSeq),
        preciseCardinality
      )
    case preciseCountDistinctDecode: PreciseCountDistinctDecode =>
      KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCountDistinctDecode.child, attributeSeq),
        preciseCountDistinctDecode
      )
    case kylinTimestampAdd: KylinTimestampAdd =>
      TimestampAddTransformer(
        ExpressionConverter
          .replaceWithExpressionTransformer(kylinTimestampAdd.left, attributeSeq),
        ExpressionConverter
          .replaceWithExpressionTransformer(kylinTimestampAdd.mid, attributeSeq),
        ExpressionConverter
          .replaceWithExpressionTransformer(kylinTimestampAdd.right, attributeSeq),
        kylinTimestampAdd
      )
    case _ =>
      throw new UnsupportedOperationException(
        s"${expr.getClass} or $expr is not currently supported.")
  }

  override def getAttrsIndexForExtensionAggregateExpr(
      aggregateFunc: AggregateFunction,
      mode: AggregateMode,
      exp: AggregateExpression,
      aggregateAttributeList: Seq[Attribute],
      aggregateAttr: ListBuffer[Attribute],
      resIndex: Int): Int = {
    var resIdx = resIndex
    exp.mode match {
      case Partial | PartialMerge =>
        val aggBufferAttr = aggregateFunc.inputAggBufferAttributes
        for (index <- aggBufferAttr.indices) {
          val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
          aggregateAttr += attr
        }
        resIdx += aggBufferAttr.size
        resIdx
      case Final | Complete =>
        aggregateAttr += aggregateAttributeList(resIdx)
        resIdx += 1
        resIdx
      case other =>
        throw new GlutenNotSupportException(s"Unsupported aggregate mode: $other.")
    }
  }

  override def buildCustomAggregateFunction(
      aggregateFunc: AggregateFunction): (Option[String], Seq[DataType]) = {
    val substraitAggFuncName = aggregateFunc match {
      case countDistinct: PreciseCountDistinct =>
        countDistinct.dataType match {
          case LongType =>
            Some("ke_bitmap_or_cardinality")
          case BinaryType =>
            Some("ke_bitmap_or_data")
          case _ =>
            throw new UnsupportedOperationException(
              s"Aggregate function ${aggregateFunc.getClass} does not support the data type " +
                s"${countDistinct.dataType}.")
        }
      case _ =>
        extensionExpressionsMapping.get(aggregateFunc.getClass)
    }
    if (substraitAggFuncName.isEmpty) {
      throw new UnsupportedOperationException(
        s"Aggregate function ${aggregateFunc.getClass} is not supported.")
    }
    (substraitAggFuncName, aggregateFunc.children.map(child => child.dataType))
  }
}
