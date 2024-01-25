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

import org.apache.gluten.backendsapi.clickhouse.CHBackendSettings
import org.apache.gluten.expression._
import org.apache.gluten.extension.ExpressionExtensionTrait
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BinaryType, DataType, LongType, StringType}
import org.apache.spark.sql.udaf._
import org.apache.spark.unsafe.types.UTF8String

import com.google.common.collect.Lists
import org.roaringbitmap.longlong.Roaring64NavigableMap

import java.util.Locale
import scala.collection.mutable.ListBuffer

class FloorDateTimeExpressionTransformer(
  substraitExprName: String,
  format: ExpressionTransformer,
  timestamp: ExpressionTransformer,
  timeZoneId: Option[String] = None,
  original: FloorDateTime) extends ExpressionTransformer with Logging {

  override def doTransform(args: java.lang.Object): ExpressionNode = {
    // The format must be constant string in the fucntion date_trunc of ch.
    if (!original.format.foldable) {
      throw new UnsupportedOperationException(
        s"The format ${original.format} must be constant string.")
    }

    val formatStr = original.format.eval().asInstanceOf[UTF8String]
    if (formatStr == null) {
      throw new UnsupportedOperationException("The format is null.")
    }

    val (newFormatStr, timeZoneIgnore) = formatStr.toString.toLowerCase(Locale.ROOT) match {
      case "second" => ("second", false)
      case "minute" => ("minute", false)
      case "hour" => ("hour", false)
      case "day" | "dd" => ("day", false)
      case "week" => ("week", true)
      case "mon" | "month" | "mm" => ("month", true)
      case "quarter" => ("quarter", true)
      case "year" | "yyyy" | "yy" => ("year", true)
      // Can not support now.
      // case "microsecond" => "microsecond"
      // case "millisecond" => "millisecond"
      case _ => throw new UnsupportedOperationException(s"The format $formatStr is invalidate.")
    }

    // Currently, data_trunc function can not support to set the specified timezone,
    // which is different with session_time_zone.
    if (
      timeZoneIgnore && timeZoneId.nonEmpty &&
        !timeZoneId.get.equalsIgnoreCase(
          SQLConf.get.getConfString(
            s"${CHBackendSettings.getBackendConfigPrefix}.runtime_config.timezone")
        )
    ) {
      throw new UnsupportedOperationException(
        s"It doesn't support trunc the format $newFormatStr with the specified timezone " +
          s"${timeZoneId.get}.")
    }

    val timestampNode = timestamp.doTransform(args)
    val lowerFormatNode = ExpressionBuilder.makeStringLiteral(newFormatStr)

    val functionMap = args.asInstanceOf[java.util.HashMap[String, java.lang.Long]]

    val dataTypes = if (timeZoneId.nonEmpty) {
      Seq(original.format.dataType, original.timestamp.dataType, StringType)
    } else {
      Seq(original.format.dataType, original.timestamp.dataType)
    }

    val functionId = ExpressionBuilder.newScalarFunction(
      functionMap,
      ConverterUtils.makeFuncName(substraitExprName, dataTypes))

    val expressionNodes = new java.util.ArrayList[ExpressionNode]()
    expressionNodes.add(lowerFormatNode)
    expressionNodes.add(timestampNode)
    if (timeZoneId != None) {
      expressionNodes.add(ExpressionBuilder.makeStringLiteral(timeZoneId.get))
    }

    val typeNode = ConverterUtils.getTypeNode(original.dataType, original.nullable)
    ExpressionBuilder.makeScalarFunction(functionId, expressionNodes, typeNode)
  }
}

class KeBitmapFunctionTransformer(
  substraitExprName: String,
  child: ExpressionTransformer,
  original: Expression) extends ExpressionTransformer with Logging {

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
    Sig[PreciseCountDistinct]("ke_bitmap_or_cardinality")
  )

  /** Replace extension expression to transformer. */
  override def replaceWithExtensionExpressionTransformer(
    substraitExprName: String,
    expr: Expression,
    attributeSeq: Seq[Attribute]): ExpressionTransformer = expr match {
    case floorDateTime: FloorDateTime =>
      new FloorDateTimeExpressionTransformer(
        substraitExprName,
        ExpressionConverter.replaceWithExpressionTransformer(floorDateTime.format, attributeSeq),
        ExpressionConverter.replaceWithExpressionTransformer(floorDateTime.timestamp, attributeSeq),
        floorDateTime.timeZoneId,
        floorDateTime
      )
    case preciseCardinality: PreciseCardinality =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCardinality.child, attributeSeq),
        preciseCardinality
      )
    case preciseCountDistinctDecode: PreciseCountDistinctDecode =>
      new KeBitmapFunctionTransformer(
        substraitExprName,
        ExpressionConverter
          .replaceWithExpressionTransformer(preciseCountDistinctDecode.child, attributeSeq),
        preciseCountDistinctDecode
      )
    case other =>
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
    var reIndex = resIndex
    aggregateFunc match {
      case bitmap
        if bitmap.getClass.getSimpleName.equals("ReusePreciseCountDistinct") ||
          bitmap.getClass.getSimpleName.equals("PreciseCountDistinctAndValue") ||
          bitmap.getClass.getSimpleName.equals("PreciseCountDistinctAndArray") ||
          bitmap.getClass.getSimpleName.equals("PreciseCountDistinct") =>
        mode match {
          case Partial =>
            val bitmapFunc = aggregateFunc
              .asInstanceOf[TypedImperativeAggregate[Roaring64NavigableMap]]
            val aggBufferAttr = bitmapFunc.inputAggBufferAttributes
            for (index <- aggBufferAttr.indices) {
              val attr = ConverterUtils.getAttrFromExpr(aggBufferAttr(index))
              aggregateAttr += attr
            }
            reIndex += aggBufferAttr.size
            reIndex
          case Final =>
            aggregateAttr += aggregateAttributeList(resIndex)
            reIndex += 1
            reIndex
          case other =>
            throw new UnsupportedOperationException(s"Unsupported aggregate mode: $other.")
        }
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
