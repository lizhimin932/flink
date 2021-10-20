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

package org.apache.flink.table.planner.codegen.calls

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.data.binary.BinaryArrayData
import org.apache.flink.table.data.util.{DataFormatConverters, MapDataUtil}
import org.apache.flink.table.data.writer.{BinaryArrayWriter, BinaryRowWriter}
import org.apache.flink.table.planner.codegen.CodeGenUtils.{binaryRowFieldSetAccess, binaryRowSetNull, binaryWriterWriteField, binaryWriterWriteNull, _}
import org.apache.flink.table.planner.codegen.GenerateUtils._
import org.apache.flink.table.planner.codegen.GeneratedExpression.{ALWAYS_NULL, NEVER_NULL, NO_CODE}
import org.apache.flink.table.planner.codegen.{CodeGenException, CodeGenUtils, CodeGeneratorContext, GeneratedExpression}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala
import org.apache.flink.table.runtime.functions.{SqlDateTimeUtils, SqlFunctionUtils}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType
import org.apache.flink.table.runtime.types.PlannerTypeUtils
import org.apache.flink.table.runtime.types.PlannerTypeUtils.{isInteroperable, isPrimitive}
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils._
import org.apache.flink.table.types.logical.LogicalTypeFamily.DATETIME
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.logical._
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsExplicitCast
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldTypes
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging.findCommonType
import org.apache.flink.util.Preconditions.checkArgument

import org.apache.calcite.avatica.util.DateTimeUtils.MILLIS_PER_DAY
import org.apache.calcite.avatica.util.{DateTimeUtils, TimeUnitRange}
import org.apache.calcite.util.BuiltInMethod

import java.lang.{StringBuilder => JStringBuilder}
import java.nio.charset.StandardCharsets
import java.util.Arrays.asList

import scala.collection.JavaConversions._

/**
  * Utilities to generate SQL scalar operators, e.g. arithmetic operator,
  * compare operator, equal operator, etc.
  */
object ScalarOperatorGens {

  // ----------------------------------------------------------------------------------------
  // scalar operators generate utils
  // ----------------------------------------------------------------------------------------

  /**
    * Generates a binary arithmetic operator, e.g. + - * / %
    */
  def generateBinaryArithmeticOperator(
    ctx: CodeGeneratorContext,
    operator: String,
    resultType: LogicalType,
    left: GeneratedExpression,
    right: GeneratedExpression)
  : GeneratedExpression = {

    resultType match {
      case dt: DecimalType =>
        return generateDecimalBinaryArithmeticOperator(ctx, operator, dt, left, right)
      case _ =>
    }

    val leftCasting = operator match {
      case "%" =>
        if (isInteroperable(left.resultType, right.resultType)) {
          numericCasting(left.resultType, resultType)
        } else {
          val castedType = if (isDecimal(left.resultType)) {
            new BigIntType()
          } else {
            left.resultType
          }
          numericCasting(left.resultType, castedType)
        }
      case _ => numericCasting(left.resultType, resultType)
    }

    val rightCasting = numericCasting(right.resultType, resultType)
    val resultTypeTerm = primitiveTypeTermForType(resultType)

    generateOperatorIfNotNull(ctx, resultType, left, right) {
      (leftTerm, rightTerm) =>
        s"($resultTypeTerm) (${leftCasting(leftTerm)} $operator ${rightCasting(rightTerm)})"
    }
  }

  /**
    * Generates a binary arithmetic operator for Decimal, e.g. + - * / %
    */
  private def generateDecimalBinaryArithmeticOperator(
      ctx: CodeGeneratorContext,
      operator: String,
      resultType: DecimalType,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {

    // do not cast a decimal operand to resultType, which may change its value.
    // use it as is during calculation.
    def castToDec(t: LogicalType): String => String = t match {
      case _: DecimalType => (operandTerm: String) => s"$operandTerm"
      case _ => numericCasting(t, resultType)
    }
    val methods = Map(
      "+" -> "add",
      "-" -> "subtract",
      "*" -> "multiply",
      "/" -> "divide",
      "%" -> "mod")

    generateOperatorIfNotNull(ctx, resultType, left, right) {
      (leftTerm, rightTerm) => {
        val method = methods(operator)
        val leftCasted = castToDec(left.resultType)(leftTerm)
        val rightCasted = castToDec(right.resultType)(rightTerm)
        val precision = resultType.getPrecision
        val scale = resultType.getScale
        s"$DECIMAL_UTIL.$method($leftCasted, $rightCasted, $precision, $scale)"
      }
    }
  }

  /**
    * Generates an unary arithmetic operator, e.g. -num
    */
  def generateUnaryArithmeticOperator(
      ctx: CodeGeneratorContext,
      operator: String,
      resultType: LogicalType,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, resultType, operand) {
      operandTerm =>
        if (isDecimal(operand.resultType) && operator == "-") {
          s"$DECIMAL_UTIL.negate($operandTerm)"
        } else if (isDecimal(operand.resultType) && operator == "+") {
          s"$operandTerm"
        } else {
          // no need to check if result type might be null,
          // because if this line of code is called, `operatorTerm` must not be null
          val typeTerm = primitiveTypeTermForType(resultType)
          s"($typeTerm) $operator($operandTerm)"
        }
    }
  }

  def generateTemporalPlusMinus(
    ctx: CodeGeneratorContext,
    plus: Boolean,
    resultType: LogicalType,
    left: GeneratedExpression,
    right: GeneratedExpression)
  : GeneratedExpression = {

    val op = if (plus) "+" else "-"

    (left.resultType.getTypeRoot, right.resultType.getTypeRoot) match {
      // arithmetic of time point and time interval
      case (INTERVAL_YEAR_MONTH, INTERVAL_YEAR_MONTH) |
           (INTERVAL_DAY_TIME, INTERVAL_DAY_TIME) =>
        generateBinaryArithmeticOperator(ctx, op, left.resultType, left, right)

      case (DATE, INTERVAL_DAY_TIME) =>
        resultType.getTypeRoot match {
          case DATE =>
            generateOperatorIfNotNull(ctx, new DateType(), left, right) {
              (l, r) => s"$l $op (java.lang.Math.toIntExact($r / ${MILLIS_PER_DAY}L))"
            }
          case TIMESTAMP_WITHOUT_TIME_ZONE =>
            generateOperatorIfNotNull(ctx, resultType, left, right) {
              (l, r) => s"$TIMESTAMP_DATA.fromEpochMillis(($l * ${MILLIS_PER_DAY}L) $op $r)"
            }
        }

      case (DATE, INTERVAL_YEAR_MONTH) =>
        generateOperatorIfNotNull(ctx, new DateType(), left, right) {
          (l, r) => s"${qualifyMethod(BuiltInMethods.ADD_MONTHS)}($l, $op($r))"
        }

      case (TIME_WITHOUT_TIME_ZONE, INTERVAL_DAY_TIME) =>
        generateOperatorIfNotNull(ctx, new TimeType(), left, right) {
          (l, r) => s"java.lang.Math.toIntExact((($l + ${MILLIS_PER_DAY}L) $op (" +
            s"java.lang.Math.toIntExact($r % ${MILLIS_PER_DAY}L))) % ${MILLIS_PER_DAY}L)"
        }

      case (TIME_WITHOUT_TIME_ZONE, INTERVAL_YEAR_MONTH) =>
        generateOperatorIfNotNull(ctx, new TimeType(), left, right) {
          (l, r) => s"$l"
        }

      case (TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTERVAL_DAY_TIME) =>
        generateOperatorIfNotNull(ctx, left.resultType, left, right) {
          (l, r) => {
            val leftTerm = s"$l.getMillisecond()"
            val nanoTerm = s"$l.getNanoOfMillisecond()"
            s"$TIMESTAMP_DATA.fromEpochMillis($leftTerm $op $r, $nanoTerm)"
          }
        }

      case (TIMESTAMP_WITHOUT_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTERVAL_YEAR_MONTH) =>
        generateOperatorIfNotNull(ctx, left.resultType, left, right) {
          (l, r) => {
            val leftTerm = s"$l.getMillisecond()"
            val nanoTerm = s"$l.getNanoOfMillisecond()"
            s"""
               |$TIMESTAMP_DATA.fromEpochMillis(
               |  ${qualifyMethod(BuiltInMethods.ADD_MONTHS)}($leftTerm, $op($r)),
               |  $nanoTerm)
             """.stripMargin
          }
        }

      // minus arithmetic of time points (i.e. for TIMESTAMPDIFF)
      case (TIMESTAMP_WITHOUT_TIME_ZONE | TIME_WITHOUT_TIME_ZONE | DATE,
      TIMESTAMP_WITHOUT_TIME_ZONE | TIME_WITHOUT_TIME_ZONE | DATE) if !plus =>
        resultType.getTypeRoot match {
          case INTERVAL_YEAR_MONTH =>
            generateOperatorIfNotNull(ctx, resultType, left, right) {
              (ll, rr) => (left.resultType.getTypeRoot, right.resultType.getTypeRoot) match {
                case (TIMESTAMP_WITHOUT_TIME_ZONE, DATE) =>
                  val leftTerm = s"$ll.getMillisecond()"
                  s"${qualifyMethod(BuiltInMethods.SUBTRACT_MONTHS)}" +
                    s"($leftTerm, $rr * ${MILLIS_PER_DAY}L)"
                case (DATE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
                  val rightTerm = s"$rr.getMillisecond()"
                  s"${qualifyMethod(BuiltInMethods.SUBTRACT_MONTHS)}" +
                    s"($ll * ${MILLIS_PER_DAY}L, $rightTerm)"
                case (TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
                  val leftTerm = s"$ll.getMillisecond()"
                  val rightTerm = s"$rr.getMillisecond()"
                  s"${qualifyMethod(BuiltInMethods.SUBTRACT_MONTHS)}($leftTerm, $rightTerm)"
                case (TIMESTAMP_WITHOUT_TIME_ZONE, TIME_WITHOUT_TIME_ZONE) =>
                  val leftTerm = s"$ll.getMillisecond()"
                  s"${qualifyMethod(BuiltInMethods.SUBTRACT_MONTHS)}($leftTerm, $rr)"
                case (TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
                  val rightTerm = s"$rr.getMillisecond()"
                  s"${qualifyMethod(BuiltInMethods.SUBTRACT_MONTHS)}($ll, $rightTerm)"
                case _ =>
                  s"${qualifyMethod(BuiltInMethods.SUBTRACT_MONTHS)}($ll, $rr)"
              }
            }

          case INTERVAL_DAY_TIME =>
            generateOperatorIfNotNull(ctx, resultType, left, right) {
              (ll, rr) => (left.resultType.getTypeRoot, right.resultType.getTypeRoot) match {
                case (TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
                  val leftTerm = s"$ll.getMillisecond()"
                  val rightTerm = s"$rr.getMillisecond()"
                  s"$leftTerm $op $rightTerm"
                case (DATE, DATE) =>
                  s"($ll * ${MILLIS_PER_DAY}L) $op ($rr * ${MILLIS_PER_DAY}L)"
                case (TIMESTAMP_WITHOUT_TIME_ZONE, DATE) =>
                  val leftTerm = s"$ll.getMillisecond()"
                  s"$leftTerm $op ($rr * ${MILLIS_PER_DAY}L)"
                case (DATE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
                  val rightTerm = s"$rr.getMillisecond()"
                  s"($ll * ${MILLIS_PER_DAY}L) $op $rightTerm"
              }
            }
        }

      // minus arithmetic of time points (i.e. for TIMESTAMPDIFF for TIMESTAMP_LTZ)
      case (TIMESTAMP_WITH_LOCAL_TIME_ZONE, t)
        if t.getFamilies.contains(DATETIME) && !plus =>
        generateTimestampLtzMinus(ctx, resultType, left, right)
      case (t, TIMESTAMP_WITH_LOCAL_TIME_ZONE)
        if t.getFamilies.contains(DATETIME) && !plus =>
        generateTimestampLtzMinus(ctx, resultType, left, right)
      case _ =>
        throw new CodeGenException("Unsupported temporal arithmetic.")
    }
  }

  private def generateTimestampLtzMinus(
     ctx: CodeGeneratorContext,
     resultType: LogicalType,
     left: GeneratedExpression,
     right: GeneratedExpression)
  : GeneratedExpression = {
    resultType.getTypeRoot match {
      case INTERVAL_YEAR_MONTH =>
        generateOperatorIfNotNull(ctx, resultType, left, right) {
          (ll, rr) => (left.resultType.getTypeRoot, right.resultType.getTypeRoot) match {
            case (TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
              val leftTerm = s"$ll.getMillisecond()"
              val rightTerm = s"$rr.getMillisecond()"
              s"${qualifyMethod(BuiltInMethods.SUBTRACT_MONTHS)}($leftTerm, $rightTerm)"
            case _ =>
              throw new CodeGenException(
                "TIMESTAMP_LTZ only supports diff between the same type.")
          }
        }

      case INTERVAL_DAY_TIME =>
        generateOperatorIfNotNull(ctx, resultType, left, right) {
          (ll, rr) => (left.resultType.getTypeRoot, right.resultType.getTypeRoot) match {
            case (TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
              val leftTerm = s"$ll.getMillisecond()"
              val rightTerm = s"$rr.getMillisecond()"
              s"$leftTerm - $rightTerm"
            case _ =>
              throw new CodeGenException(
                "TIMESTAMP_LTZ only supports diff between the same type.")
          }
        }
      case _ =>
        throw new CodeGenException("Unsupported temporal arithmetic.")
    }
  }

  def generateUnaryIntervalPlusMinus(
    ctx: CodeGeneratorContext,
    plus: Boolean,
    operand: GeneratedExpression)
  : GeneratedExpression = {
    val operator = if (plus) "+" else "-"
    generateUnaryArithmeticOperator(ctx, operator, operand.resultType, operand)
  }

  // ----------------------------------------------------------------------------------------
  // scalar expression generate utils
  // ----------------------------------------------------------------------------------------

  /**
    * Generates IN expression using a HashSet
    */
  def generateIn(
      ctx: CodeGeneratorContext,
      needle: GeneratedExpression,
      haystack: Seq[GeneratedExpression])
    : GeneratedExpression = {

    // add elements to hash set if they are constant
    if (haystack.forall(_.literal)) {

      // determine common numeric type
      val widerType = toScala(findCommonType(asList(needle.resultType, haystack.head.resultType)))
        .orElse(throw new CodeGenException(s"Unable to find common type of $needle and $haystack."))

      // we need to normalize the values for the hash set
      val castNumeric = widerType match {
        case Some(t) => (value: GeneratedExpression) =>
          numericCasting(value.resultType, t)(value.resultTerm)
        case None => (value: GeneratedExpression) => value.resultTerm
      }

      val resultType = widerType match {
        case Some(t) => t
        case None => needle.resultType
      }

      val elements = haystack.map { element =>
        element.copy(
          castNumeric(element), // cast element to wider type
          element.nullTerm,
          element.code,
          resultType)
      }
      val setTerm = ctx.addReusableHashSet(elements, resultType)

      val castedNeedle = needle.copy(
        castNumeric(needle), // cast needle to wider type
        needle.nullTerm,
        needle.code,
        resultType)

      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
      val resultTypeTerm = primitiveTypeTermForType(new BooleanType())
      val defaultValue = primitiveDefaultValue(new BooleanType())

      val operatorCode = if (ctx.nullCheck) {
        s"""
           |${castedNeedle.code}
           |$resultTypeTerm $resultTerm = $defaultValue;
           |boolean $nullTerm = true;
           |if (!${castedNeedle.nullTerm}) {
           |  $resultTerm = $setTerm.contains(${castedNeedle.resultTerm});
           |  $nullTerm = !$resultTerm && $setTerm.containsNull();
           |}
           |""".stripMargin.trim
      }
      else {
        s"""
           |${castedNeedle.code}
           |$resultTypeTerm $resultTerm = $setTerm.contains(${castedNeedle.resultTerm});
           |""".stripMargin.trim
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, new BooleanType())
    } else {
      // we use a chain of ORs for a set that contains non-constant elements
      haystack
        .map(generateEquals(ctx, needle, _))
        .reduce((left, right) =>
          generateOr(ctx, left, right)
        )
    }
  }

  def generateEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    val canEqual = isInteroperable(left.resultType, right.resultType)
    if (isCharacterString(left.resultType) && isCharacterString(right.resultType)) {
      generateOperatorIfNotNull(ctx, new BooleanType(), left, right) {
        (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
      }
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison(ctx, "==", left, right)
    }
    // array types
    else if (isArray(left.resultType) && canEqual) {
      generateArrayComparison(ctx, left, right)
    }
    // map types
    else if (isMap(left.resultType) && canEqual) {
      val mapType = left.resultType.asInstanceOf[MapType]
      generateMapComparison(ctx, left, right, mapType.getKeyType, mapType.getValueType)
    }
    // multiset types
    else if (isMultiset(left.resultType) && canEqual) {
      val multisetType = left.resultType.asInstanceOf[MultisetType]
      generateMapComparison(ctx, left, right, multisetType.getElementType, new IntType(false))
    }
    // comparable types of same type
    else if (isComparable(left.resultType) && canEqual) {
      generateComparison(ctx, "==", left, right)
    }
    // generic types of same type
    else if (isRaw(left.resultType) && canEqual) {
      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
      val genericSer = ctx.addReusableTypeSerializer(left.resultType)
      val ser = s"$genericSer.getInnerSerializer()"
      val resultType = new BooleanType()
      val code = s"""
         |${left.code}
         |${right.code}
         |boolean $nullTerm = ${left.nullTerm} || ${right.nullTerm};
         |boolean $resultTerm = ${primitiveDefaultValue(resultType)};
         |if (!$nullTerm) {
         |  ${left.resultTerm}.ensureMaterialized($ser);
         |  ${right.resultTerm}.ensureMaterialized($ser);
         |  $resultTerm =
         |    ${left.resultTerm}.getBinarySection().equals(${right.resultTerm}.getBinarySection());
         |}
         |""".stripMargin
      GeneratedExpression(resultTerm, nullTerm, code, resultType)
    }
    // support date/time/timestamp equalTo string.
    // for performance, we cast literal string to literal time.
    else if (isTimePoint(left.resultType) && isCharacterString(right.resultType)) {
      if (right.literal) {
        generateEquals(ctx, left, generateCastStringLiteralToDateTime(ctx, right, left.resultType))
      } else {
        generateEquals(ctx, left, generateCast(ctx, right, left.resultType))
      }
    }
    else if (isTimePoint(right.resultType) && isCharacterString(left.resultType)) {
      if (left.literal) {
        generateEquals(
          ctx,
          generateCastStringLiteralToDateTime(ctx, left, right.resultType),
          right)
      } else {
        generateEquals(ctx, generateCast(ctx, left, right.resultType), right)
      }
    }
    // non comparable types
    else {
      generateOperatorIfNotNull(ctx, new BooleanType(), left, right) {
        if (isReference(left.resultType)) {
          (leftTerm, rightTerm) => s"$leftTerm.equals($rightTerm)"
        }
        else if (isReference(right.resultType)) {
          (leftTerm, rightTerm) => s"$rightTerm.equals($leftTerm)"
        }
        else {
          throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
        }
      }
    }
  }

  def generateIsNotDistinctFrom(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOr(ctx,
      generateAnd(ctx, generateIsNull(ctx, left), generateIsNull(ctx, right)),
      generateEquals(ctx, left, right))
  }

  def generateNotEquals(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    if (isCharacterString(left.resultType) && isCharacterString(right.resultType)) {
      generateOperatorIfNotNull(ctx, new BooleanType(), left, right) {
        (leftTerm, rightTerm) => s"!$leftTerm.equals($rightTerm)"
      }
    }
    // numeric types
    else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
      generateComparison(ctx, "!=", left, right)
    }
    // temporal types
    else if (isTemporal(left.resultType) &&
        isInteroperable(left.resultType, right.resultType)) {
      generateComparison(ctx, "!=", left, right)
    }
    // array types
    else if (isArray(left.resultType) && isInteroperable(left.resultType, right.resultType)) {
      val equalsExpr = generateEquals(ctx, left, right)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})", equalsExpr.nullTerm, equalsExpr.code, new BooleanType())
    }
    // map types
    else if (isMap(left.resultType) && isInteroperable(left.resultType, right.resultType)) {
      val equalsExpr = generateEquals(ctx, left, right)
      GeneratedExpression(
        s"(!${equalsExpr.resultTerm})", equalsExpr.nullTerm, equalsExpr.code, new BooleanType())
    }
    // comparable types
    else if (isComparable(left.resultType) &&
        isInteroperable(left.resultType, right.resultType)) {
      generateComparison(ctx, "!=", left, right)
    }
    // non-comparable types
    else {
      generateOperatorIfNotNull(ctx, new BooleanType(), left, right) {
        if (isReference(left.resultType)) {
          (leftTerm, rightTerm) => s"!($leftTerm.equals($rightTerm))"
        }
        else if (isReference(right.resultType)) {
          (leftTerm, rightTerm) => s"!($rightTerm.equals($leftTerm))"
        }
        else {
          throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
            s"${right.resultType}")
        }
      }
    }
  }

  /**
    * Generates comparison code for numeric types and comparable types of same type.
    */
  def generateComparison(
      ctx: CodeGeneratorContext,
      operator: String,
      left: GeneratedExpression,
      right: GeneratedExpression)
    : GeneratedExpression = {
    generateOperatorIfNotNull(ctx, new BooleanType(), left, right) {
      // either side is decimal
      if (isDecimal(left.resultType) || isDecimal(right.resultType)) {
        (leftTerm, rightTerm) => {
          s"$DECIMAL_UTIL.compare($leftTerm, $rightTerm) $operator 0"
        }
      }
      // both sides are numeric
      else if (isNumeric(left.resultType) && isNumeric(right.resultType)) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }

      // both sides are timestamp
      else if (isTimestamp(left.resultType) && isTimestamp(right.resultType)) {
        (leftTerm, rightTerm) =>
          s"$leftTerm.compareTo($rightTerm) $operator 0"
      }

      // both sides are timestamp with local zone
      else if (isTimestampWithLocalZone(left.resultType) &&
          isTimestampWithLocalZone(right.resultType)) {
        (leftTerm, rightTerm) =>
          s"$leftTerm.compareTo($rightTerm) $operator 0"
      }

      // both sides are temporal of same type
      else if (isTemporal(left.resultType) &&
          isInteroperable(left.resultType, right.resultType)) {
        (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
      }
      // both sides are boolean
      else if (isBoolean(left.resultType) &&
          isInteroperable(left.resultType, right.resultType)) {
        operator match {
          case "==" | "!=" => (leftTerm, rightTerm) => s"$leftTerm $operator $rightTerm"
          case ">" | "<" | "<=" | ">=" =>
            (leftTerm, rightTerm) =>
              s"java.lang.Boolean.compare($leftTerm, $rightTerm) $operator 0"
          case _ => throw new CodeGenException(s"Unsupported boolean comparison '$operator'.")
        }
      }
      // both sides are binary type
      else if (isBinaryString(left.resultType) &&
          isInteroperable(left.resultType, right.resultType)) {
        val utilName = classOf[SqlFunctionUtils].getCanonicalName
        (leftTerm, rightTerm) =>
          s"$utilName.byteArrayCompare($leftTerm, $rightTerm) $operator 0"
      }
      // both sides are same comparable type
      else if (isComparable(left.resultType) &&
          isInteroperable(left.resultType, right.resultType)) {
        (leftTerm, rightTerm) =>
          s"(($leftTerm == null) ? (($rightTerm == null) ? 0 : -1) : (($rightTerm == null) ? " +
            s"1 : ($leftTerm.compareTo($rightTerm)))) $operator 0"
      }
      else {
        throw new CodeGenException(s"Incomparable types: ${left.resultType} and " +
          s"${right.resultType}")
      }
    }
  }

  def generateIsNull(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression): GeneratedExpression = {
    if (ctx.nullCheck) {
      GeneratedExpression(operand.nullTerm, NEVER_NULL, operand.code, new BooleanType(false))
    }
    else if (!ctx.nullCheck && isReference(operand.resultType)) {
      val resultTerm = newName("isNull")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = ${operand.resultTerm} == null;
           |""".stripMargin
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, new BooleanType(false))
    }
    else {
      GeneratedExpression("false", NEVER_NULL, operand.code, new BooleanType(false))
    }
  }

  def generateIsNotNull(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression): GeneratedExpression = {
    if (ctx.nullCheck) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = !${operand.nullTerm};
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, new BooleanType(false))
    }
    else if (!ctx.nullCheck && isReference(operand.resultType)) {
      val resultTerm = newName("result")
      val operatorCode =
        s"""
           |${operand.code}
           |boolean $resultTerm = ${operand.resultTerm} != null;
           |""".stripMargin.trim
      GeneratedExpression(resultTerm, NEVER_NULL, operatorCode, new BooleanType(false))
    }
    else {
      GeneratedExpression("true", NEVER_NULL, operand.code, new BooleanType(false))
    }
  }

  def generateAnd(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")

    val operatorCode = if (ctx.nullCheck) {
      // Three-valued logic:
      // no Unknown -> Two-valued logic
      // True && Unknown -> Unknown
      // False && Unknown -> False
      // Unknown && True -> Unknown
      // Unknown && False -> False
      // Unknown && Unknown -> Unknown
      s"""
         |${left.code}
         |
         |boolean $resultTerm = false;
         |boolean $nullTerm = false;
         |if (!${left.nullTerm} && !${left.resultTerm}) {
         |  // left expr is false, skip right expr
         |} else {
         |  ${right.code}
         |
         |  if (!${left.nullTerm} && !${right.nullTerm}) {
         |    $resultTerm = ${left.resultTerm} && ${right.resultTerm};
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = false;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = false;
         |  }
         |  else {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |}
       """.stripMargin.trim
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = false;
         |if (${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, new BooleanType())
  }

  def generateOr(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")

    val operatorCode = if (ctx.nullCheck) {
      // Three-valued logic:
      // no Unknown -> Two-valued logic
      // True || Unknown -> True
      // False || Unknown -> Unknown
      // Unknown || True -> True
      // Unknown || False -> Unknown
      // Unknown || Unknown -> Unknown
      s"""
         |${left.code}
         |
        |boolean $resultTerm = true;
         |boolean $nullTerm = false;
         |if (!${left.nullTerm} && ${left.resultTerm}) {
         |  // left expr is true, skip right expr
         |} else {
         |  ${right.code}
         |
        |  if (!${left.nullTerm} && !${right.nullTerm}) {
         |    $resultTerm = ${left.resultTerm} || ${right.resultTerm};
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && ${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = true;
         |    $nullTerm = false;
         |  }
         |  else if (!${left.nullTerm} && !${left.resultTerm} && ${right.nullTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && ${right.resultTerm}) {
         |    $resultTerm = true;
         |    $nullTerm = false;
         |  }
         |  else if (${left.nullTerm} && !${right.nullTerm} && !${right.resultTerm}) {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |  else {
         |    $resultTerm = false;
         |    $nullTerm = true;
         |  }
         |}
         |""".stripMargin.trim
    }
    else {
      s"""
         |${left.code}
         |boolean $resultTerm = true;
         |if (!${left.resultTerm}) {
         |  ${right.code}
         |  $resultTerm = ${right.resultTerm};
         |}
         |""".stripMargin.trim
    }

    GeneratedExpression(resultTerm, nullTerm, operatorCode, new BooleanType())
  }

  def generateNot(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression)
    : GeneratedExpression = {
    // Three-valued logic:
    // no Unknown -> Two-valued logic
    // Unknown -> Unknown
    generateUnaryOperatorIfNotNull(ctx, new BooleanType(), operand) {
      operandTerm => s"!($operandTerm)"
    }
  }

  def generateIsTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      operand.resultTerm, // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      new BooleanType())
  }

  def generateIsNotTrue(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm})", // unknown is always false by default
      GeneratedExpression.NEVER_NULL,
      operand.code,
      new BooleanType())
  }

  def generateIsFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(!${operand.resultTerm} && !${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      new BooleanType())
  }

  def generateIsNotFalse(operand: GeneratedExpression): GeneratedExpression = {
    GeneratedExpression(
      s"(${operand.resultTerm} || ${operand.nullTerm})",
      GeneratedExpression.NEVER_NULL,
      operand.code,
      new BooleanType())
  }

  def generateReinterpret(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      targetType: LogicalType)
    : GeneratedExpression = (operand.resultType.getTypeRoot, targetType.getTypeRoot) match {

    case (_, _) if isInteroperable(operand.resultType, targetType) =>
      operand.copy(resultType = targetType)

    // internal reinterpretation of temporal types
    // Date -> Integer
    // Time -> Integer
    // Timestamp -> Long
    // Integer -> Date
    // Integer -> Time
    // Long -> Timestamp
    // Integer -> Interval Months
    // Long -> Interval Millis
    // Interval Months -> Integer
    // Interval Millis -> Long
    // Date -> Long
    // Time -> Long
    // Interval Months -> Long
    case (DATE, INTEGER) |
         (TIME_WITHOUT_TIME_ZONE, INTEGER) |
         (INTEGER, DATE) |
         (INTEGER, TIME_WITHOUT_TIME_ZONE) |
         (INTEGER, INTERVAL_YEAR_MONTH) |
         (BIGINT, INTERVAL_DAY_TIME) |
         (INTERVAL_YEAR_MONTH, INTEGER) |
         (INTERVAL_DAY_TIME, BIGINT) |
         (DATE, BIGINT) |
         (TIME_WITHOUT_TIME_ZONE, BIGINT) |
         (INTERVAL_YEAR_MONTH, BIGINT) =>
      internalExprCasting(operand, targetType)

    case (TIMESTAMP_WITHOUT_TIME_ZONE, BIGINT) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$operandTerm.getMillisecond()"
      }

    case (BIGINT, TIMESTAMP_WITHOUT_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$TIMESTAMP_DATA.fromEpochMillis($operandTerm)"
      }

    case (from, to) =>
      if (from == to) {
        operand
      } else {
        throw new CodeGenException(s"Unsupported reinterpret from '$from' to '$to'.")
      }
  }

  def generateCast(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      targetType: LogicalType)
    : GeneratedExpression = (operand.resultType.getTypeRoot, targetType.getTypeRoot) match {

    // special case: cast from TimeIndicatorTypeInfo to SqlTimeTypeInfo
    case (TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE)
      if operand.resultType.asInstanceOf[TimestampType].getKind == TimestampKind.PROCTIME ||
          operand.resultType.asInstanceOf[TimestampType].getKind == TimestampKind.ROWTIME ||
          targetType.asInstanceOf[TimestampType].getKind == TimestampKind.PROCTIME ||
          targetType.asInstanceOf[TimestampType].getKind == TimestampKind.ROWTIME =>
        // just replace the DataType
        operand.copy(resultType = new TimestampType(operand.resultType.isNullable, 3))

    case (TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
      val fromType = operand.resultType.asInstanceOf[TimestampType]
      val toType = targetType.asInstanceOf[TimestampType]
      if (fromType.getPrecision <= toType.getPrecision) {
        operand.copy(resultType = targetType)
      } else {
        val method = qualifyMethod(BuiltInMethods.TRUNCATE_SQL_TIMESTAMP)
        generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
          operandTerm =>
            s"$method($operandTerm, ${toType.getPrecision})"
        }
      }

    case (TIMESTAMP_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
      val fromType = operand.resultType.asInstanceOf[TimestampType]
      val toType = targetType.asInstanceOf[LocalZonedTimestampType]
      val method = qualifyMethod(BuiltInMethods.TIMESTAMP_TO_TIMESTAMP_WITH_LOCAL_ZONE)
      if (fromType.getPrecision < toType.getPrecision) {
        generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
          operandTerm =>
            val timeZone = ctx.addReusableSessionTimeZone()
            s"$method($operandTerm, $timeZone)"
        }
      } else {
        val truncate_method = qualifyMethod(BuiltInMethods.TRUNCATE_SQL_TIMESTAMP)
        generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
          operandTerm =>
            val timeZone = ctx.addReusableSessionTimeZone()
            s"$truncate_method($method($operandTerm, $timeZone), ${toType.getPrecision})"
        }
      }

    case (TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
      val fromType = operand.resultType.asInstanceOf[LocalZonedTimestampType]
      val toType = targetType.asInstanceOf[TimestampType]
      val method = qualifyMethod(BuiltInMethods.TIMESTAMP_WITH_LOCAL_ZONE_TO_TIMESTAMP)
      if (fromType.getPrecision < toType.getPrecision) {
        generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
          operandTerm =>
            val zone = ctx.addReusableSessionTimeZone()
            s"$method($operandTerm, $zone)"
        }
      } else {
        val truncate_method = qualifyMethod(BuiltInMethods.TRUNCATE_SQL_TIMESTAMP)
        generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
          operandTerm =>
            val zone = ctx.addReusableSessionTimeZone()
            s"$truncate_method($method($operandTerm, $zone), ${toType.getPrecision})"
        }
      }

    case (TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
      val fromType = operand.resultType.asInstanceOf[LocalZonedTimestampType]
      val toType = targetType.asInstanceOf[LocalZonedTimestampType]
      if (fromType.getPrecision <= toType.getPrecision) {
        operand.copy(resultType = targetType)
      } else {
        val method = qualifyMethod(BuiltInMethods.TRUNCATE_SQL_TIMESTAMP)
        generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
          operandTerm =>
            s"$method($operandTerm, ${toType.getPrecision})"
        }
      }

    // identity casting
    case (_, _) if isInteroperable(operand.resultType, targetType) =>
      operand.copy(resultType = targetType)

    // Date/Time/Timestamp -> String
    case (_, VARCHAR | CHAR) if TypeCheckUtils.isTimePoint(operand.resultType) =>
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand), targetType) {
        operandTerm =>
          s"${localTimeToStringCode(ctx, operand.resultType, operandTerm.head)}"
      }

    // Interval Months -> String
    case (INTERVAL_YEAR_MONTH, VARCHAR | CHAR) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_YEAR_MONTH_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.YEAR_TO_MONTH)
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand), targetType) {
        terms => s"$method(${terms.head}, $timeUnitRange)"
      }

    // Interval Millis -> String
    case (INTERVAL_DAY_TIME, VARCHAR | CHAR) =>
      val method = qualifyMethod(BuiltInMethod.INTERVAL_DAY_TIME_TO_STRING.method)
      val timeUnitRange = qualifyEnum(TimeUnitRange.DAY_TO_SECOND)
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand), targetType) {
        terms => s"$method(${terms.head}, $timeUnitRange, 3)" // milli second precision
      }

    // Array -> String
    case (ARRAY, VARCHAR | CHAR) =>
      generateCastArrayToString(
        ctx, operand, operand.resultType.asInstanceOf[ArrayType], targetType)

    // Byte array -> String UTF-8
    case (BINARY | VARBINARY, VARCHAR | CHAR) =>
      val charset = classOf[StandardCharsets].getCanonicalName
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand), targetType) {
        terms => s"(new String(${terms.head}, $charset.UTF_8))"
      }


    // Map -> String
    case (MAP, VARCHAR | CHAR) =>
      generateCastMapToString(
        ctx, operand, operand.resultType.asInstanceOf[MapType], targetType)

    // composite type -> String
    case (ROW, VARCHAR | CHAR) =>
      generateCastRowDataToString(
        ctx, operand, operand.resultType.asInstanceOf[RowType], targetType)

    case (RAW, VARCHAR | CHAR) =>
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand), targetType) {
        terms =>
          val converter = DataFormatConverters.getConverterForDataType(
            fromLogicalTypeToDataType(operand.resultType))
          val converterTerm = ctx.addReusableObject(converter, "converter")
          s""" "" + $converterTerm.toExternal(${terms.head})"""
      }

    case (RAW, BINARY | VARBINARY) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        val serializer = operand.resultType.asInstanceOf[RawType[_]].getTypeSerializer
        val serTerm = ctx.addReusableObject(serializer, "serializer")
        operandTerm => s"$operandTerm.toBytes($serTerm)"
      }

    // * (not Date/Time/Timestamp) -> String
    // TODO: GenericType with Date/Time/Timestamp -> String would call toString implicitly
    case (_, VARCHAR | CHAR) =>
      generateStringResultCallIfArgsNotNull(ctx, Seq(operand), targetType) {
        terms => s""" "" + ${terms.head}"""
      }

    // String -> Boolean
    case (VARCHAR | CHAR, BOOLEAN) =>
      val castedExpression = generateUnaryOperatorIfNotNull(
        ctx,
        targetType,
        operand,
        resultNullable = true) {
        operandTerm => s"$BINARY_STRING_UTIL.toBooleanSQL($operandTerm)"
      }
      val resultTerm = newName("primitiveCastResult")
      castedExpression.copy(
        resultTerm = resultTerm,
        code =
          s"""
             |${castedExpression.code}
             |boolean $resultTerm = Boolean.TRUE.equals(${castedExpression.resultTerm});
             |""".stripMargin
      )

    // String -> NUMERIC TYPE (not Character)
    case (VARCHAR | CHAR, _)
      if TypeCheckUtils.isNumeric(targetType) =>
      targetType match {
        case dt: DecimalType =>
          generateUnaryOperatorIfNotNull(ctx, targetType, operand) { operandTerm =>
            s"$BINARY_STRING_UTIL.toDecimal($operandTerm, ${dt.getPrecision}, ${dt.getScale})"
          }
        case _ =>
          val methodName = targetType.getTypeRoot match {
            case TINYINT => "toByte"
            case SMALLINT => "toShort"
            case INTEGER => "toInt"
            case BIGINT => "toLong"
            case DOUBLE => "toDouble"
            case FLOAT => "toFloat"
            case _ => null
          }
          assert(methodName != null, "Unexpected data type.")
          generateUnaryOperatorIfNotNull(
            ctx,
            targetType,
            operand,
            resultNullable = true) {
            operandTerm => s"($BINARY_STRING_UTIL.$methodName($operandTerm.trim()))"
          }
      }

    // String -> Date
    case (VARCHAR | CHAR, DATE) =>
      generateUnaryOperatorIfNotNull(
        ctx,
        targetType,
        operand,
        resultNullable = true) {
        operandTerm =>
          s"${qualifyMethod(BuiltInMethods.STRING_TO_DATE)}($operandTerm.toString())"
      }

    // String -> Time
    case (VARCHAR | CHAR, TIME_WITHOUT_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(
        ctx,
        targetType,
        operand,
        resultNullable = true) {
        operandTerm =>
          s"${qualifyMethod(BuiltInMethods.STRING_TO_TIME)}($operandTerm.toString())"
      }

    // String -> Timestamp
    case (VARCHAR | CHAR, TIMESTAMP_WITHOUT_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(
        ctx,
        targetType,
        operand,
        resultNullable = true) {
        operandTerm =>
          s"""
             |${qualifyMethod(BuiltInMethods.STRING_TO_TIMESTAMP)}($operandTerm.toString())
           """.stripMargin
      }

    case (VARCHAR | CHAR, TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
      generateCallWithStmtIfArgsNotNull(
        ctx, targetType, Seq(operand), resultNullable = true) { operands =>
        val zone = ctx.addReusableSessionTimeZone()
        val method = qualifyMethod(BuiltInMethods.STRING_TO_TIMESTAMP_TIME_ZONE)
        val toTimestampResultName = newName("toTimestampResult")
        // this method call might return null
        val stmt = s"Long $toTimestampResultName = $method(${operands.head}.toString(), $zone);"
        val result =
          s"""
             |($toTimestampResultName == null ?
             |  null :
             |  $TIMESTAMP_DATA.fromEpochMillis($toTimestampResultName))
             |""".stripMargin
        (stmt, result)
      }

    // String -> binary
    case (VARCHAR | CHAR, VARBINARY | BINARY) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$operandTerm.toBytes()"
      }

    // Note: SQL2003 $6.12 - casting is not allowed between boolean and numeric types.
    //       Calcite does not allow it either.

    // Boolean -> DECIMAL
    case (BOOLEAN, DECIMAL) =>
      val dt = targetType.asInstanceOf[DecimalType]
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$DECIMAL_UTIL.castFrom($operandTerm, ${dt.getPrecision}, ${dt.getScale})"
      }

    // Boolean -> NUMERIC TYPE
    case (BOOLEAN, _) if TypeCheckUtils.isNumeric(targetType) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"($targetTypeTerm) ($operandTerm ? 1 : 0)"
      }

    // DECIMAL -> Boolean
    case (DECIMAL, BOOLEAN) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$DECIMAL_UTIL.castToBoolean($operandTerm)"
      }

    // NUMERIC TYPE -> Boolean
    case (_, BOOLEAN) if isNumeric(operand.resultType) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$operandTerm != 0"
      }

    // between NUMERIC TYPE | Decimal
    case  (_, _) if isNumeric(operand.resultType) && isNumeric(targetType) =>
      val operandCasting = numericCasting(operand.resultType, targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"${operandCasting(operandTerm)}"
      }

    // Date -> Timestamp
    case (DATE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm =>
          s"""
             |$TIMESTAMP_DATA.fromEpochMillis(
             |  $operandTerm * ${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)
           """.stripMargin
      }

    // Timestamp -> Date
    case (TIMESTAMP_WITHOUT_TIME_ZONE, DATE) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm =>
          s"""
             |($targetTypeTerm) ($operandTerm.getMillisecond() /
             |  ${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)
           """.stripMargin
      }

    // Time -> Timestamp
    case (TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITHOUT_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm => s"$TIMESTAMP_DATA.fromEpochMillis($operandTerm)"
      }

    // Timestamp -> Time
    case (TIMESTAMP_WITHOUT_TIME_ZONE, TIME_WITHOUT_TIME_ZONE) =>
      val targetTypeTerm = primitiveTypeTermForType(targetType)
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) {
        operandTerm =>
          s"($targetTypeTerm) ($operandTerm.getMillisecond() % " +
            s"${classOf[DateTimeUtils].getCanonicalName}.MILLIS_PER_DAY)"
      }

    // Date -> Timestamp with local time zone
    case (DATE, TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) { operandTerm =>
        val zone = ctx.addReusableSessionTimeZone()
        val method = qualifyMethod(BuiltInMethods.DATE_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE)
        s"$TIMESTAMP_DATA.fromEpochMillis($method($operandTerm, $zone))"
      }

    // Timestamp with local time zone -> Date
    case (TIMESTAMP_WITH_LOCAL_TIME_ZONE, DATE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) { operandTerm =>
        val zone = ctx.addReusableSessionTimeZone()
        val method = qualifyMethod(BuiltInMethods.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_DATE)
        s"$method($operandTerm.getMillisecond(), $zone)"
      }

    // Time -> Timestamp with local time zone
    case (TIME_WITHOUT_TIME_ZONE, TIMESTAMP_WITH_LOCAL_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) { operandTerm =>
        val zone = ctx.addReusableSessionTimeZone()
        val method = qualifyMethod(BuiltInMethods.TIME_TO_TIMESTAMP_WITH_LOCAL_TIME_ZONE)
        s"$TIMESTAMP_DATA.fromEpochMillis($method($operandTerm, $zone))"
      }

    // Timestamp with local time zone -> Time
    case (TIMESTAMP_WITH_LOCAL_TIME_ZONE, TIME_WITHOUT_TIME_ZONE) =>
      generateUnaryOperatorIfNotNull(ctx, targetType, operand) { operandTerm =>
        val zone = ctx.addReusableSessionTimeZone()
        val method = qualifyMethod(BuiltInMethods.TIMESTAMP_WITH_LOCAL_TIME_ZONE_TO_TIME)
        s"$method($operandTerm.getMillisecond(), $zone)"
      }

    // Disable cast conversion between Numeric type and Timestamp type
    case (TINYINT, TIMESTAMP_WITHOUT_TIME_ZONE) |
         (SMALLINT, TIMESTAMP_WITHOUT_TIME_ZONE) |
         (INTEGER, TIMESTAMP_WITHOUT_TIME_ZONE) |
         (BIGINT, TIMESTAMP_WITHOUT_TIME_ZONE) |
         (FLOAT, TIMESTAMP_WITHOUT_TIME_ZONE) |
         (DOUBLE, TIMESTAMP_WITHOUT_TIME_ZONE) |
         (DECIMAL, TIMESTAMP_WITHOUT_TIME_ZONE) |
         (TIMESTAMP_WITHOUT_TIME_ZONE, TINYINT) |
         (TIMESTAMP_WITHOUT_TIME_ZONE, SMALLINT) |
         (TIMESTAMP_WITHOUT_TIME_ZONE, INTEGER) |
         (TIMESTAMP_WITHOUT_TIME_ZONE, BIGINT) |
         (TIMESTAMP_WITHOUT_TIME_ZONE, FLOAT) |
         (TIMESTAMP_WITHOUT_TIME_ZONE, DOUBLE) |
         (TIMESTAMP_WITHOUT_TIME_ZONE, DECIMAL) => {
      if (TIMESTAMP_WITHOUT_TIME_ZONE.equals(targetType.getTypeRoot)) {
        throw new ValidationException("The cast conversion from NUMERIC type to TIMESTAMP type" +
          " is not allowed, it's recommended to use TO_TIMESTAMP(FROM_UNIXTIME(numeric_col))" +
          " instead, note the numeric is in seconds.")
      } else {
        throw new ValidationException("The cast conversion from TIMESTAMP type to NUMERIC type" +
          " is not allowed, it's recommended to use" +
          " UNIX_TIMESTAMP(CAST(timestamp_col AS STRING)) instead.")
      }
    }

    // Disable cast conversion between Numeric type and TimestampLtz type
    case (TINYINT, TIMESTAMP_WITH_LOCAL_TIME_ZONE) |
         (SMALLINT, TIMESTAMP_WITH_LOCAL_TIME_ZONE) |
         (INTEGER, TIMESTAMP_WITH_LOCAL_TIME_ZONE) |
         (BIGINT, TIMESTAMP_WITH_LOCAL_TIME_ZONE) |
         (FLOAT, TIMESTAMP_WITH_LOCAL_TIME_ZONE) |
         (DOUBLE, TIMESTAMP_WITH_LOCAL_TIME_ZONE) |
         (DECIMAL, TIMESTAMP_WITH_LOCAL_TIME_ZONE) |
         (TIMESTAMP_WITH_LOCAL_TIME_ZONE, TINYINT) |
         (TIMESTAMP_WITH_LOCAL_TIME_ZONE, SMALLINT) |
         (TIMESTAMP_WITH_LOCAL_TIME_ZONE, INTEGER) |
         (TIMESTAMP_WITH_LOCAL_TIME_ZONE, BIGINT) |
         (TIMESTAMP_WITH_LOCAL_TIME_ZONE, FLOAT) |
         (TIMESTAMP_WITH_LOCAL_TIME_ZONE, DOUBLE) |
         (TIMESTAMP_WITH_LOCAL_TIME_ZONE, DECIMAL) => {
      if (TIMESTAMP_WITH_LOCAL_TIME_ZONE.equals(targetType.getTypeRoot)) {
        throw new ValidationException("The cast conversion from NUMERIC type" +
          " to TIMESTAMP_LTZ type is not allowed, it's recommended to use" +
          " TO_TIMESTAMP_LTZ(numeric_col, precision) instead.")
      } else {
        throw new ValidationException("The cast conversion from" +
          " TIMESTAMP_LTZ type to NUMERIC type is not allowed.")
      }
    }

    // internal temporal casting
    // Date -> Integer
    // Time -> Integer
    // Integer -> Date
    // Integer -> Time
    // Integer -> Interval Months
    // Long -> Interval Millis
    // Interval Months -> Integer
    // Interval Millis -> Long
    case (DATE, INTEGER) |
         (TIME_WITHOUT_TIME_ZONE, INTEGER) |
         (INTEGER, DATE) |
         (INTEGER, TIME_WITHOUT_TIME_ZONE) |
         (INTEGER, INTERVAL_YEAR_MONTH) |
         (BIGINT, INTERVAL_DAY_TIME) |
         (INTERVAL_YEAR_MONTH, INTEGER) |
         (INTERVAL_DAY_TIME, BIGINT) =>
      internalExprCasting(operand, targetType)

    // internal reinterpretation of temporal types
    // Date, Time, Interval Months -> Long
    case  (DATE, BIGINT)
          | (TIME_WITHOUT_TIME_ZONE, BIGINT)
          | (INTERVAL_YEAR_MONTH, BIGINT) =>
      internalExprCasting(operand, targetType)

    case (ROW | STRUCTURED_TYPE, ROW | STRUCTURED_TYPE)
        if supportsExplicitCast(operand.resultType, targetType) =>
      generateCastRowToRow(ctx, operand, targetType)

    case (_, _) =>
      throw new CodeGenException(s"Unsupported cast from '${operand.resultType}' to '$targetType'.")
  }

  def generateIfElse(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression],
      resultType: LogicalType,
      i: Int = 0)
    : GeneratedExpression = {
    // else part
    if (i == operands.size - 1) {
      generateCast(ctx, operands(i), resultType)
    }
    else {
      // check that the condition is boolean
      // we do not check for null instead we use the default value
      // thus null is false
      requireBoolean(operands(i))
      val condition = operands(i)
      val trueAction = generateCast(ctx, operands(i + 1), resultType)
      val falseAction = generateIfElse(ctx, operands, resultType, i + 2)

      val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
      val resultTypeTerm = primitiveTypeTermForType(resultType)
      val defaultValue = primitiveDefaultValue(resultType)

      val operatorCode = if (ctx.nullCheck) {
        s"""
           |${condition.code}
           |$resultTypeTerm $resultTerm = $defaultValue;
           |boolean $nullTerm;
           |if (${condition.resultTerm}) {
           |  ${trueAction.code}
           |  $nullTerm = ${trueAction.nullTerm};
           |  if (!$nullTerm) {
           |    $resultTerm = ${trueAction.resultTerm};
           |  }
           |}
           |else {
           |  ${falseAction.code}
           |  $nullTerm = ${falseAction.nullTerm};
           |  if (!$nullTerm) {
           |    $resultTerm = ${falseAction.resultTerm};
           |  }
           |}
           |""".stripMargin.trim
      }
      else {
        s"""
           |${condition.code}
           |$resultTypeTerm $resultTerm;
           |if (${condition.resultTerm}) {
           |  ${trueAction.code}
           |  $resultTerm = ${trueAction.resultTerm};
           |}
           |else {
           |  ${falseAction.code}
           |  $resultTerm = ${falseAction.resultTerm};
           |}
           |""".stripMargin.trim
      }

      GeneratedExpression(resultTerm, nullTerm, operatorCode, resultType)
    }
  }

  def generateDot(
      ctx: CodeGeneratorContext,
      operands: Seq[GeneratedExpression]): GeneratedExpression = {

    // due to https://issues.apache.org/jira/browse/CALCITE-2162, expression such as
    // "array[1].a.b" won't work now.
    if (operands.size > 2) {
      throw new CodeGenException(
        "A DOT operator with more than 2 operands is not supported yet.")
    }

    checkArgument(operands(1).literal)
    checkArgument(isCharacterString(operands(1).resultType))
    checkArgument(operands.head.resultType.isInstanceOf[RowType])

    val fieldName = operands(1).literalValue.get.toString
    val fieldIdx = operands
      .head
      .resultType
      .asInstanceOf[RowType]
      .getFieldIndex(fieldName)

    val access = generateFieldAccess(
      ctx,
      operands.head.resultType,
      operands.head.resultTerm,
      fieldIdx)

    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val resultTypeTerm = primitiveTypeTermForType(access.resultType)
    val defaultValue = primitiveDefaultValue(access.resultType)

    val resultCode = if (ctx.nullCheck) {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |$resultTypeTerm $resultTerm;
         |boolean $nullTerm;
         |if (${operands.map(_.nullTerm).mkString(" || ")}) {
         |  $resultTerm = $defaultValue;
         |  $nullTerm = true;
         |}
         |else {
         |  ${access.code}
         |  $resultTerm = ${access.resultTerm};
         |  $nullTerm = ${access.nullTerm};
         |}
         |""".stripMargin
    } else {
      s"""
         |${operands.map(_.code).mkString("\n")}
         |${access.code}
         |$resultTypeTerm $resultTerm = ${access.resultTerm};
         |""".stripMargin
    }


    GeneratedExpression(
      resultTerm,
      nullTerm,
      resultCode,
      access.resultType
    )
  }

  // ----------------------------------------------------------------------------------------
  // value construction and accessing generate utils
  // ----------------------------------------------------------------------------------------

  def generateRow(
      ctx: CodeGeneratorContext,
      rowType: LogicalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {
    val fieldTypes = getFieldTypes(rowType)
    val isLiteral = elements.forall(e => e.literal)
    val isPrimitive = fieldTypes.forall(PlannerTypeUtils.isPrimitive)

    if (isLiteral) {
      // generate literal row
      generateLiteralRow(ctx, rowType, elements)
    } else {
      if (isPrimitive) {
        // generate primitive row
        val mapped = elements.zipWithIndex.map { case (element, idx) =>
          if (element.literal) {
            element
          } else {
            val tpe = fieldTypes(idx)
            val resultTerm = primitiveDefaultValue(tpe)
            GeneratedExpression(resultTerm, ALWAYS_NULL, NO_CODE, tpe, Some(null))
          }
        }
        val row = generateLiteralRow(ctx, rowType, mapped)
        val code = elements.zipWithIndex.map { case (element, idx) =>
          val tpe = fieldTypes(idx)
          if (element.literal) {
            ""
          } else if(ctx.nullCheck) {
            s"""
               |${element.code}
               |if (${element.nullTerm}) {
               |  ${binaryRowSetNull(idx, row.resultTerm, tpe)};
               |} else {
               |  ${binaryRowFieldSetAccess(idx, row.resultTerm, tpe, element.resultTerm)};
               |}
           """.stripMargin
          } else {
            s"""
               |${element.code}
               |${binaryRowFieldSetAccess(idx, row.resultTerm, tpe, element.resultTerm)};
           """.stripMargin
          }
        }.mkString("\n")
        GeneratedExpression(row.resultTerm, NEVER_NULL, code, rowType)
      } else {
        // generate general row
        generateNonLiteralRow(ctx, rowType, elements)
      }
    }
  }

  private def generateLiteralRow(
      ctx: CodeGeneratorContext,
      rowType: LogicalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {
    checkArgument(elements.forall(e => e.literal))
    val expr = generateNonLiteralRow(ctx, rowType, elements)
    ctx.addReusableInitStatement(expr.code)
    GeneratedExpression(expr.resultTerm, GeneratedExpression.NEVER_NULL, NO_CODE, rowType)
  }

  private def generateNonLiteralRow(
      ctx: CodeGeneratorContext,
      rowType: LogicalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {
    val fieldTypes = getFieldTypes(rowType)

    val rowTerm = newName("row")
    val writerTerm = newName("writer")
    val writerCls = className[BinaryRowWriter]

    val writeCode = elements.zipWithIndex.map {
      case (element, idx) =>
        val tpe = fieldTypes(idx)
        if (ctx.nullCheck) {
          s"""
             |${element.code}
             |if (${element.nullTerm}) {
             |  ${binaryWriterWriteNull(idx, writerTerm, tpe)};
             |} else {
             |  ${binaryWriterWriteField(ctx, idx, element.resultTerm, writerTerm, tpe)};
             |}
           """.stripMargin
        } else {
          s"""
             |${element.code}
             |${binaryWriterWriteField(ctx, idx, element.resultTerm, writerTerm, tpe)};
           """.stripMargin
        }
    }.mkString("\n")

    val code =
      s"""
         |$writerTerm.reset();
         |$writeCode
         |$writerTerm.complete();
       """.stripMargin

    ctx.addReusableMember(s"$BINARY_ROW $rowTerm = new $BINARY_ROW(${fieldTypes.length});")
    ctx.addReusableMember(s"$writerCls $writerTerm = new $writerCls($rowTerm);")
    GeneratedExpression(rowTerm, GeneratedExpression.NEVER_NULL, code, rowType)
  }

  def generateArray(
      ctx: CodeGeneratorContext,
      resultType: LogicalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {

    checkArgument(resultType.isInstanceOf[ArrayType])
    val arrayType = resultType.asInstanceOf[ArrayType]
    val elementType = arrayType.getElementType
    val isLiteral = elements.forall(e => e.literal)
    val isPrimitive = PlannerTypeUtils.isPrimitive(elementType)

    if (isLiteral) {
      // generate literal array
      generateLiteralArray(ctx, arrayType, elements)
    } else {
      if (isPrimitive) {
        // generate primitive array
        val mapped = elements.map { element =>
          if (element.literal) {
            element
          } else {
            val resultTerm = primitiveDefaultValue(elementType)
            GeneratedExpression(resultTerm, ALWAYS_NULL, NO_CODE, elementType, Some(null))
          }
        }
        val array = generateLiteralArray(ctx, arrayType, mapped)
        val code = generatePrimitiveArrayUpdateCode(ctx, array.resultTerm, elementType, elements)
        GeneratedExpression(array.resultTerm, GeneratedExpression.NEVER_NULL, code, arrayType)
      } else {
        // generate general array
        generateNonLiteralArray(ctx, arrayType, elements)
      }
    }
  }

  private def generatePrimitiveArrayUpdateCode(
      ctx: CodeGeneratorContext,
      arrayTerm: String,
      elementType: LogicalType,
      elements: Seq[GeneratedExpression]): String = {
    elements.zipWithIndex.map { case (element, idx) =>
      if (element.literal) {
        ""
      } else if (ctx.nullCheck) {
        s"""
           |${element.code}
           |if (${element.nullTerm}) {
           |  ${binaryArraySetNull(idx, arrayTerm, elementType)};
           |} else {
           |  ${binaryRowFieldSetAccess(
          idx, arrayTerm, elementType, element.resultTerm)};
           |}
             """.stripMargin
      } else {
        s"""
           |${element.code}
           |${binaryRowFieldSetAccess(
          idx, arrayTerm, elementType, element.resultTerm)};
             """.stripMargin
      }
    }.mkString("\n")
  }

  private def generateLiteralArray(
      ctx: CodeGeneratorContext,
      arrayType: ArrayType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {
    checkArgument(elements.forall(e => e.literal))
    val expr = generateNonLiteralArray(ctx, arrayType, elements)
    ctx.addReusableInitStatement(expr.code)
    GeneratedExpression(expr.resultTerm, GeneratedExpression.NEVER_NULL, NO_CODE, arrayType)
  }

  private def generateNonLiteralArray(
      ctx: CodeGeneratorContext,
      arrayType: ArrayType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {

    val elementType = arrayType.getElementType
    val arrayTerm = newName("array")
    val writerTerm = newName("writer")
    val writerCls = className[BinaryArrayWriter]
    val elementSize = BinaryArrayData.calculateFixLengthPartSize(elementType)

    val writeCode = elements.zipWithIndex.map {
      case (element, idx) =>
        s"""
           |${element.code}
           |if (${element.nullTerm}) {
           |  ${binaryArraySetNull(idx, writerTerm, elementType)};
           |} else {
           |  ${binaryWriterWriteField(ctx, idx, element.resultTerm, writerTerm, elementType)};
           |}
          """.stripMargin
    }.mkString("\n")

    val code =
      s"""
         |$writerTerm.reset();
         |$writeCode
         |$writerTerm.complete();
         """.stripMargin

    val memberStmt =
      s"""
         |$BINARY_ARRAY $arrayTerm = new $BINARY_ARRAY();
         |$writerCls $writerTerm = new $writerCls($arrayTerm, ${elements.length}, $elementSize);
       """.stripMargin

    ctx.addReusableMember(memberStmt)
    GeneratedExpression(arrayTerm, GeneratedExpression.NEVER_NULL, code, arrayType)
  }

  /**
   * Return null when array index out of bounds which follows Calcite's behaviour.
   * @see [[org.apache.calcite.sql.fun.SqlStdOperatorTable.ITEM]]
   */
  def generateArrayElementAt(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression,
      index: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val componentInfo = array.resultType.asInstanceOf[ArrayType].getElementType
    val resultTypeTerm = primitiveTypeTermForType(componentInfo)
    val defaultTerm = primitiveDefaultValue(componentInfo)

    index.literalValue match {
      case Some(v: Int) if v < 1 =>
        throw new ValidationException(
          s"Array element access needs an index starting at 1 but was $v.")
      case _ => //nothing
    }
    val idxStr = s"${index.resultTerm} - 1"
    val arrayIsNull = s"${array.resultTerm}.isNullAt($idxStr)"
    val arrayGet =
      rowFieldReadAccess(ctx, idxStr, array.resultTerm, componentInfo)

    val arrayAccessCode =
    s"""
        |${array.code}
        |${index.code}
        |boolean $nullTerm = ${array.nullTerm} || ${index.nullTerm} ||
        |   $idxStr < 0 || $idxStr >= ${array.resultTerm}.size() || $arrayIsNull;
        |$resultTypeTerm $resultTerm = $nullTerm ? $defaultTerm : $arrayGet;
        |""".stripMargin
    GeneratedExpression(resultTerm, nullTerm, arrayAccessCode, componentInfo)
  }

  def generateArrayElement(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val resultType = array.resultType.asInstanceOf[ArrayType].getElementType
    val resultTypeTerm = primitiveTypeTermForType(resultType)
    val defaultValue = primitiveDefaultValue(resultType)

    val arrayLengthCode = s"${array.nullTerm} ? 0 : ${array.resultTerm}.size()"

    val arrayGet = rowFieldReadAccess(ctx, 0, array.resultTerm, resultType)
    val arrayAccessCode =
      s"""
         |${array.code}
         |boolean $nullTerm;
         |$resultTypeTerm $resultTerm;
         |switch ($arrayLengthCode) {
         |  case 0:
         |    $nullTerm = true;
         |    $resultTerm = $defaultValue;
         |    break;
         |  case 1:
         |    $nullTerm = ${array.resultTerm}.isNullAt(0);
         |    $resultTerm = $nullTerm ? $defaultValue : $arrayGet;
         |    break;
         |  default:
         |    throw new RuntimeException("Array has more than one element.");
         |}
         |""".stripMargin

    GeneratedExpression(resultTerm, nullTerm, arrayAccessCode, resultType)
  }

  def generateArrayCardinality(
      ctx: CodeGeneratorContext,
      array: GeneratedExpression)
    : GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, new IntType(), array) {
      _ => s"${array.resultTerm}.size()"
    }
  }

  /**
   * Returns the greatest or least value of the list of arguments,
   * returns NULL if any argument is NULL.
   */
  def generateGreatestLeast(
      resultType: LogicalType,
      elements: Seq[GeneratedExpression],
      greatest: Boolean = true)
  : GeneratedExpression = {
    val Seq(result, cur, nullTerm) = newNames("result", "cur", "nullTerm")
    val widerType = toScala(findCommonType(elements.map(element => element.resultType)))
      .orElse(throw new CodeGenException(s"Unable to find common type for $elements."))
    val resultTypeTerm = boxedTypeTermForType(widerType.get)

    def castIfNumeric(t: GeneratedExpression): String = {
      if (TypeCheckUtils.isNumeric(widerType.get)) {
         s"${numericCasting(t.resultType, widerType.get).apply(t.resultTerm)}"
      } else {
         s"${t.resultTerm}"
      }
    }

    val elementsCode = elements.map { element =>
      s"""
         | ${element.code}
         | if (!$nullTerm) {
         |   $resultTypeTerm $cur = ${castIfNumeric(element)};
         |   if (${element.nullTerm}) {
         |     $nullTerm = true;
         |   } else {
         |     int compareResult = $result.compareTo($cur);
         |     if (($greatest && compareResult < 0) || (compareResult > 0 && !$greatest)) {
         |       $result = $cur;
         |     }
         |   }
         | }
       """.stripMargin
    }.mkString("\n")

    val code =
      s"""
         | $resultTypeTerm $result = ${castIfNumeric(elements.head)};
         | boolean $nullTerm = false;
         | $elementsCode
         | if ($nullTerm) {
         |   $result = null;
         | }
       """.stripMargin
    GeneratedExpression(result, nullTerm, code, resultType)
  }

  def generateMap(
      ctx: CodeGeneratorContext,
      resultType: LogicalType,
      elements: Seq[GeneratedExpression]): GeneratedExpression = {

    checkArgument(resultType.isInstanceOf[MapType])
    val mapType = resultType.asInstanceOf[MapType]
    val baseMap = newName("map")

    // prepare map key array
    val keyElements = elements.grouped(2).map { case Seq(key, _) => key }.toSeq
    val keyType = mapType.getKeyType
    val keyExpr = generateArray(ctx, new ArrayType(keyType), keyElements)
    val isKeyFixLength = isPrimitive(keyType)

    // prepare map value array
    val valueElements = elements.grouped(2).map { case Seq(_, value) => value }.toSeq
    val valueType = mapType.getValueType
    val valueExpr = generateArray(ctx, new ArrayType(valueType), valueElements)
    val isValueFixLength = isPrimitive(valueType)

    // construct binary map
    ctx.addReusableMember(s"$MAP_DATA $baseMap = null;")

    val code = if (isKeyFixLength && isValueFixLength) {
      val binaryMap = newName("binaryMap")
      ctx.addReusableMember(s"$BINARY_MAP $binaryMap = null;")
      // the key and value are fixed length, initialize and reuse the map in constructor
      val init =
        s"$binaryMap = $BINARY_MAP.valueOf(${keyExpr.resultTerm}, ${valueExpr.resultTerm});"
      ctx.addReusableInitStatement(init)
      // there are some non-literal primitive fields need to update
      val keyArrayTerm = newName("keyArray")
      val valueArrayTerm = newName("valueArray")
      val keyUpdate = generatePrimitiveArrayUpdateCode(
        ctx, keyArrayTerm, keyType, keyElements)
      val valueUpdate = generatePrimitiveArrayUpdateCode(
        ctx, valueArrayTerm, valueType, valueElements)
      s"""
         |$BINARY_ARRAY $keyArrayTerm = $binaryMap.keyArray();
         |$keyUpdate
         |$BINARY_ARRAY $valueArrayTerm = $binaryMap.valueArray();
         |$valueUpdate
         |$baseMap = $binaryMap;
       """.stripMargin
    } else {
      // the key or value is not fixed length, re-create the map on every update
      s"""
         |${keyExpr.code}
         |${valueExpr.code}
         |$baseMap = $BINARY_MAP.valueOf(${keyExpr.resultTerm}, ${valueExpr.resultTerm});
       """.stripMargin
    }
    GeneratedExpression(baseMap, NEVER_NULL, code, resultType)
  }

  def generateMapGet(
      ctx: CodeGeneratorContext,
      map: GeneratedExpression,
      key: GeneratedExpression): GeneratedExpression = {
    val Seq(resultTerm, nullTerm) = newNames("result", "isNull")
    val tmpKey = newName("key")
    val length = newName("length")
    val keys = newName("keys")
    val values = newName("values")
    val index = newName("index")
    val found = newName("found")
    val tmpValue = newName("value")

    val mapType = map.resultType.asInstanceOf[MapType]
    val keyType = mapType.getKeyType
    val valueType = mapType.getValueType

    // use primitive for key as key is not null
    val keyTypeTerm = primitiveTypeTermForType(keyType)
    val valueTypeTerm = primitiveTypeTermForType(valueType)
    val valueDefault = primitiveDefaultValue(valueType)
    val binaryMapTerm = newName("binaryMap")
    val genericMapTerm = newName("genericMap")
    val boxedValueTypeTerm = boxedTypeTermForType(valueType)

    val mapTerm = map.resultTerm

    val equal = generateEquals(
      ctx,
      // We have to create a new GeneratedExpression from `key`, but erase the code of it.
      // Otherwise, the code of `key` will be called twice in `accessCode`, which may lead to
      // exceptions such as 'Redefinition of local variable'.
      GeneratedExpression(key.resultTerm, key.nullTerm, NO_CODE, key.resultType, key.literalValue),
      GeneratedExpression(tmpKey, NEVER_NULL, NO_CODE, keyType)
    )
    val code =
      s"""
         |if ($mapTerm instanceof $BINARY_MAP) {
         |  $BINARY_MAP $binaryMapTerm = ($BINARY_MAP) $mapTerm;
         |  final int $length = $binaryMapTerm.size();
         |  final $BINARY_ARRAY $keys = $binaryMapTerm.keyArray();
         |  final $BINARY_ARRAY $values = $binaryMapTerm.valueArray();
         |
         |  int $index = 0;
         |  boolean $found = false;
         |  if (${key.nullTerm}) {
         |    while ($index < $length && !$found) {
         |      if ($keys.isNullAt($index)) {
         |        $found = true;
         |      } else {
         |        $index++;
         |      }
         |    }
         |  } else {
         |    while ($index < $length && !$found) {
         |      final $keyTypeTerm $tmpKey = ${rowFieldReadAccess(ctx, index, keys, keyType)};
         |      ${equal.code}
         |      if (${equal.resultTerm}) {
         |        $found = true;
         |      } else {
         |        $index++;
         |      }
         |    }
         |  }
         |
         |  if (!$found || $values.isNullAt($index)) {
         |    $nullTerm = true;
         |  } else {
         |    $resultTerm = ${rowFieldReadAccess(ctx, index, values, valueType)};
         |  }
         |} else {
         |  $GENERIC_MAP $genericMapTerm = ($GENERIC_MAP) $mapTerm;
         |  $boxedValueTypeTerm $tmpValue =
         |    ($boxedValueTypeTerm) $genericMapTerm.get(($keyTypeTerm) ${key.resultTerm});
         |  if ($tmpValue == null) {
         |    $nullTerm = true;
         |  } else {
         |    $resultTerm = $tmpValue;
         |  }
         |}
        """.stripMargin

    val accessCode =
      s"""
         |${map.code}
         |${key.code}
         |boolean $nullTerm = (${map.nullTerm} || ${key.nullTerm});
         |$valueTypeTerm $resultTerm = $valueDefault;
         |if (!$nullTerm) {
         | $code
         |}
        """.stripMargin

    GeneratedExpression(resultTerm, nullTerm, accessCode, valueType)
  }

  def generateMapCardinality(
      ctx: CodeGeneratorContext,
      map: GeneratedExpression): GeneratedExpression = {
    generateUnaryOperatorIfNotNull(ctx, new IntType(), map) {
      _ => s"${map.resultTerm}.size()"
    }
  }

  // ----------------------------------------------------------------------------------------
  // private generate utils
  // ----------------------------------------------------------------------------------------

  private def generateCastRowToRow(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      targetRowType: LogicalType)
    : GeneratedExpression = {
    // assumes that the arity has been checked before
    generateCallWithStmtIfArgsNotNull(ctx, targetRowType, Seq(operand)) { case Seq(rowTerm) =>
      val fieldExprs = operand
        .resultType
        .getChildren
        .zip(targetRowType.getChildren)
        .zipWithIndex
        .map { case ((sourceType, targetType), idx) =>
          val sourceTypeTerm = primitiveTypeTermForType(sourceType)
          val sourceTerm = newName("field")
          val sourceAccessCode = rowFieldReadAccess(ctx, idx, rowTerm, sourceType)
          val sourceExpr = GeneratedExpression(
            sourceTerm,
            s"$rowTerm.isNullAt($idx)",
            s"$sourceTypeTerm $sourceTerm = ($sourceTypeTerm) $sourceAccessCode;",
            sourceType)
          generateCast(ctx, sourceExpr, targetType)
        }

      val generateRowExpr = generateRow(ctx, targetRowType, fieldExprs)
      (generateRowExpr.code, generateRowExpr.resultTerm)
    }
  }

  private def generateCastStringLiteralToDateTime(
      ctx: CodeGeneratorContext,
      stringLiteral: GeneratedExpression,
      expectType: LogicalType): GeneratedExpression = {
    checkArgument(stringLiteral.literal)
    if (java.lang.Boolean.valueOf(stringLiteral.nullTerm)) {
      return generateNullLiteral(expectType, nullCheck = true)
    }

    val stringValue = stringLiteral.literalValue.get.toString
    val literalCode = expectType.getTypeRoot match {
      case DATE =>
        SqlDateTimeUtils.dateStringToUnixDate(stringValue) match {
          case null => throw new ValidationException(s"String '$stringValue' is not a valid date")
          case v => v
        }
      case TIME_WITHOUT_TIME_ZONE =>
        SqlDateTimeUtils.timeStringToUnixDate(stringValue) match {
          case null => throw new ValidationException(s"String '$stringValue' is not a valid time")
          case v => v
        }
      case TIMESTAMP_WITHOUT_TIME_ZONE =>
        SqlDateTimeUtils.toTimestampData(stringValue) match {
          case null =>
            throw new ValidationException(s"String '$stringValue' is not a valid timestamp")
          case v => s"${CodeGenUtils.TIMESTAMP_DATA}.fromEpochMillis(" +
            s"${v.getMillisecond}L, ${v.getNanoOfMillisecond})"
        }
      case _ => throw new UnsupportedOperationException
    }

    val typeTerm = primitiveTypeTermForType(expectType)
    val resultTerm = newName("stringToTime")
    val stmt = s"$typeTerm $resultTerm = $literalCode;"
    ctx.addReusableMember(stmt)
    GeneratedExpression(resultTerm, "false", "", expectType)
  }

  private def generateCastArrayToString(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      at: ArrayType,
      targetType: LogicalType): GeneratedExpression =
    generateStringResultCallWithStmtIfArgsNotNull(ctx, Seq(operand), targetType) {
      terms =>
        val builderCls = classOf[JStringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"""$builderCls $builderTerm = new $builderCls();""")

        val arrayTerm = terms.head

        val indexTerm = newName("i")
        val numTerm = newName("num")

        val elementType = at.getElementType
        val elementCls = primitiveTypeTermForType(elementType)
        val elementTerm = newName("element")
        val elementNullTerm = newName("isNull")
        val elementCode =
          s"""
             |$elementCls $elementTerm = ${primitiveDefaultValue(elementType)};
             |boolean $elementNullTerm = $arrayTerm.isNullAt($indexTerm);
             |if (!$elementNullTerm) {
             |  $elementTerm = ($elementCls) ${
            rowFieldReadAccess(ctx, indexTerm, arrayTerm, elementType)};
             |}
             """.stripMargin
        val elementExpr = GeneratedExpression(
          elementTerm, elementNullTerm, elementCode, elementType)
        val castExpr = generateCast(ctx, elementExpr, targetType)

        val stmt =
          s"""
             |$builderTerm.setLength(0);
             |$builderTerm.append("[");
             |int $numTerm = $arrayTerm.size();
             |for (int $indexTerm = 0; $indexTerm < $numTerm; $indexTerm++) {
             |  if ($indexTerm != 0) {
             |    $builderTerm.append(", ");
             |  }
             |
             |  ${castExpr.code}
             |  if (${castExpr.nullTerm}) {
             |    $builderTerm.append("null");
             |  } else {
             |    $builderTerm.append(${castExpr.resultTerm});
             |  }
             |}
             |$builderTerm.append("]");
             """.stripMargin
        (stmt, s"$builderTerm.toString()")
    }

  private def generateCastMapToString(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      mt: MapType,
      targetType: LogicalType): GeneratedExpression =
    generateStringResultCallWithStmtIfArgsNotNull(ctx, Seq(operand), targetType) {
      terms =>
        val resultTerm = newName("toStringResult")

        val builderCls = classOf[JStringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"$builderCls $builderTerm = new $builderCls();")

        val mapTerm = terms.head
        val genericMapTerm = newName("genericMap")
        val binaryMapTerm = newName("binaryMap")
        val keyArrayTerm = newName("keyArray")
        val valueArrayTerm = newName("valueArray")

        val indexTerm = newName("i")
        val numTerm = newName("num")

        val keyType = mt.getKeyType
        val keyCls = primitiveTypeTermForType(keyType)
        val keyTerm = newName("key")
        val keyNullTerm = newName("isNull")
        val keyCode =
          s"""
             |$keyCls $keyTerm = ${primitiveDefaultValue(keyType)};
             |boolean $keyNullTerm = $keyArrayTerm.isNullAt($indexTerm);
             |if (!$keyNullTerm) {
             |  $keyTerm = ($keyCls) ${
            rowFieldReadAccess(ctx, indexTerm, keyArrayTerm, keyType)};
             |}
             """.stripMargin
        val keyExpr = GeneratedExpression(keyTerm, keyNullTerm, keyCode, keyType)
        val keyCastExpr = generateCast(ctx, keyExpr, targetType)

        val valueType = mt.getValueType
        val valueCls = primitiveTypeTermForType(valueType)
        val valueTerm = newName("value")
        val valueNullTerm = newName("isNull")
        val valueCode =
          s"""
             |$valueCls $valueTerm = ${primitiveDefaultValue(valueType)};
             |boolean $valueNullTerm = $valueArrayTerm.isNullAt($indexTerm);
             |if (!$valueNullTerm) {
             |  $valueTerm = ($valueCls) ${
            rowFieldReadAccess(ctx, indexTerm, valueArrayTerm, valueType)};
             |}
             """.stripMargin
        val valueExpr = GeneratedExpression(valueTerm, valueNullTerm, valueCode, valueType)
        val valueCastExpr = generateCast(ctx, valueExpr, targetType)

        val stmt =
          s"""
             |String $resultTerm;
             |if ($mapTerm instanceof $BINARY_MAP) {
             |  $BINARY_MAP $binaryMapTerm = ($BINARY_MAP) $mapTerm;
             |  $ARRAY_DATA $keyArrayTerm = $binaryMapTerm.keyArray();
             |  $ARRAY_DATA $valueArrayTerm = $binaryMapTerm.valueArray();
             |
             |  $builderTerm.setLength(0);
             |  $builderTerm.append("{");
             |
             |  int $numTerm = $binaryMapTerm.size();
             |  for (int $indexTerm = 0; $indexTerm < $numTerm; $indexTerm++) {
             |    if ($indexTerm != 0) {
             |      $builderTerm.append(", ");
             |    }
             |
             |    ${keyCastExpr.code}
             |    if (${keyCastExpr.nullTerm}) {
             |      $builderTerm.append("null");
             |    } else {
             |      $builderTerm.append(${keyCastExpr.resultTerm});
             |    }
             |    $builderTerm.append("=");
             |
             |    ${valueCastExpr.code}
             |    if (${valueCastExpr.nullTerm}) {
             |      $builderTerm.append("null");
             |    } else {
             |      $builderTerm.append(${valueCastExpr.resultTerm});
             |    }
             |  }
             |  $builderTerm.append("}");
             |
             |  $resultTerm = $builderTerm.toString();
             |} else {
             |  $GENERIC_MAP $genericMapTerm = ($GENERIC_MAP) $mapTerm;
             |  $resultTerm = $genericMapTerm.toString();
             |}
             """.stripMargin
        (stmt, resultTerm)
    }

  private def generateCastRowDataToString(
      ctx: CodeGeneratorContext,
      operand: GeneratedExpression,
      brt: RowType,
      targetType: LogicalType): GeneratedExpression =
    generateStringResultCallWithStmtIfArgsNotNull(ctx, Seq(operand), targetType) {
      terms =>
        val builderCls = classOf[JStringBuilder].getCanonicalName
        val builderTerm = newName("builder")
        ctx.addReusableMember(s"""$builderCls $builderTerm = new $builderCls();""")

        val rowTerm = terms.head

        val appendCode = brt.getChildren.zipWithIndex.map {
          case (elementType, idx) =>
            val elementCls = primitiveTypeTermForType(elementType)
            val elementTerm = newName("element")
            val elementExpr = GeneratedExpression(
              elementTerm, s"$rowTerm.isNullAt($idx)",
              s"$elementCls $elementTerm = ($elementCls) ${rowFieldReadAccess(
                ctx, idx, rowTerm, elementType)};", elementType)
            val castExpr = generateCast(ctx, elementExpr, targetType)
            s"""
               |${if (idx != 0) s"""$builderTerm.append(",");""" else ""}
               |${castExpr.code}
               |if (${castExpr.nullTerm}) {
               |  $builderTerm.append("null");
               |} else {
               |  $builderTerm.append(${castExpr.resultTerm});
               |}
               """.stripMargin
        }.mkString("\n")

        val stmt =
          s"""
             |$builderTerm.setLength(0);
             |$builderTerm.append("(");
             |$appendCode
             |$builderTerm.append(")");
             """.stripMargin
        (stmt, s"$builderTerm.toString()")
    }

  private def generateArrayComparison(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression): GeneratedExpression =
    generateCallWithStmtIfArgsNotNull(ctx, new BooleanType(), Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)

        val resultTerm = newName("compareResult")

        val elementType = left.resultType.asInstanceOf[ArrayType].getElementType
        val elementCls = primitiveTypeTermForType(elementType)
        val elementDefault = primitiveDefaultValue(elementType)

        val leftElementTerm = newName("leftElement")
        val leftElementNullTerm = newName("leftElementIsNull")
        val leftElementExpr =
          GeneratedExpression(leftElementTerm, leftElementNullTerm, "", elementType)

        val rightElementTerm = newName("rightElement")
        val rightElementNullTerm = newName("rightElementIsNull")
        val rightElementExpr =
          GeneratedExpression(rightElementTerm, rightElementNullTerm, "", elementType)

        val indexTerm = newName("index")
        val elementEqualsExpr = generateEquals(ctx, leftElementExpr, rightElementExpr)

        val stmt =
          s"""
             |boolean $resultTerm;
             |if ($leftTerm instanceof $BINARY_ARRAY && $rightTerm instanceof $BINARY_ARRAY) {
             |  $resultTerm = $leftTerm.equals($rightTerm);
             |} else {
             |  if ($leftTerm.size() == $rightTerm.size()) {
             |    $resultTerm = true;
             |    for (int $indexTerm = 0; $indexTerm < $leftTerm.size(); $indexTerm++) {
             |      $elementCls $leftElementTerm = $elementDefault;
             |      boolean $leftElementNullTerm = $leftTerm.isNullAt($indexTerm);
             |      if (!$leftElementNullTerm) {
             |        $leftElementTerm =
             |          ${rowFieldReadAccess(ctx, indexTerm, leftTerm, elementType)};
             |      }
             |
             |      $elementCls $rightElementTerm = $elementDefault;
             |      boolean $rightElementNullTerm = $rightTerm.isNullAt($indexTerm);
             |      if (!$rightElementNullTerm) {
             |        $rightElementTerm =
             |          ${rowFieldReadAccess(ctx, indexTerm, rightTerm, elementType)};
             |      }
             |
             |      ${elementEqualsExpr.code}
             |      if (!${elementEqualsExpr.resultTerm}) {
             |        $resultTerm = false;
             |        break;
             |      }
             |    }
             |  } else {
             |    $resultTerm = false;
             |  }
             |}
             """.stripMargin
        (stmt, resultTerm)
    }

  private def generateMapComparison(
      ctx: CodeGeneratorContext,
      left: GeneratedExpression,
      right: GeneratedExpression,
      keyType: LogicalType,
      valueType: LogicalType)
    : GeneratedExpression =
    generateCallWithStmtIfArgsNotNull(ctx, new BooleanType(), Seq(left, right)) {
      args =>
        val leftTerm = args.head
        val rightTerm = args(1)

        val resultTerm = newName("compareResult")

        val mapCls = className[java.util.Map[_, _]]
        val keyCls = boxedTypeTermForType(keyType)
        val valueCls = boxedTypeTermForType(valueType)

        val leftMapTerm = newName("leftMap")
        val leftKeyTerm = newName("leftKey")
        val leftValueTerm = newName("leftValue")
        val leftValueNullTerm = newName("leftValueIsNull")
        val leftValueExpr =
          GeneratedExpression(leftValueTerm, leftValueNullTerm, "", valueType)

        val rightMapTerm = newName("rightMap")
        val rightValueTerm = newName("rightValue")
        val rightValueNullTerm = newName("rightValueIsNull")
        val rightValueExpr =
          GeneratedExpression(rightValueTerm, rightValueNullTerm, "", valueType)

        val entryTerm = newName("entry")
        val entryCls = classOf[java.util.Map.Entry[AnyRef, AnyRef]].getCanonicalName
        val valueEqualsExpr = generateEquals(ctx, leftValueExpr, rightValueExpr)

        val internalTypeCls = classOf[LogicalType].getCanonicalName
        val keyTypeTerm = ctx.addReusableObject(keyType, "keyType", internalTypeCls)
        val valueTypeTerm = ctx.addReusableObject(valueType, "valueType", internalTypeCls)
        val mapDataUtil = className[MapDataUtil]

        val stmt =
          s"""
             |boolean $resultTerm;
             |if ($leftTerm.size() == $rightTerm.size()) {
             |  $resultTerm = true;
             |  $mapCls $leftMapTerm = $mapDataUtil
             |      .convertToJavaMap($leftTerm, $keyTypeTerm, $valueTypeTerm);
             |  $mapCls $rightMapTerm = $mapDataUtil
             |      .convertToJavaMap($rightTerm, $keyTypeTerm, $valueTypeTerm);
             |
             |  for ($entryCls $entryTerm : $leftMapTerm.entrySet()) {
             |    $keyCls $leftKeyTerm = ($keyCls) $entryTerm.getKey();
             |    if ($rightMapTerm.containsKey($leftKeyTerm)) {
             |      $valueCls $leftValueTerm = ($valueCls) $entryTerm.getValue();
             |      $valueCls $rightValueTerm = ($valueCls) $rightMapTerm.get($leftKeyTerm);
             |      boolean $leftValueNullTerm = ($leftValueTerm == null);
             |      boolean $rightValueNullTerm = ($rightValueTerm == null);
             |
             |      ${valueEqualsExpr.code}
             |      if (!${valueEqualsExpr.resultTerm}) {
             |        $resultTerm = false;
             |        break;
             |      }
             |    } else {
             |      $resultTerm = false;
             |      break;
             |    }
             |  }
             |} else {
             |  $resultTerm = false;
             |}
             """.stripMargin
        (stmt, resultTerm)
    }

  // ------------------------------------------------------------------------------------------

  private def generateUnaryOperatorIfNotNull(
      ctx: CodeGeneratorContext,
      returnType: LogicalType,
      operand: GeneratedExpression,
      resultNullable: Boolean = false)
      (expr: String => String): GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(operand), resultNullable) {
      args => expr(args.head)
    }
  }

  private def generateOperatorIfNotNull(
      ctx: CodeGeneratorContext,
      returnType: LogicalType,
      left: GeneratedExpression,
      right: GeneratedExpression,
      resultNullable: Boolean = false)
      (expr: (String, String) => String)
    : GeneratedExpression = {
    generateCallIfArgsNotNull(ctx, returnType, Seq(left, right), resultNullable) {
      args => expr(args.head, args(1))
    }
  }

  // ----------------------------------------------------------------------------------------------

  private def internalExprCasting(
      expr: GeneratedExpression,
      targetType: LogicalType)
    : GeneratedExpression = {
    expr.copy(resultType = targetType)
  }

  private def numericCasting(
      operandType: LogicalType,
      resultType: LogicalType): String => String = {

    val resultTypeTerm = primitiveTypeTermForType(resultType)

    def decToPrimMethod(targetType: LogicalType): String = targetType.getTypeRoot match {
      case TINYINT => "castToByte"
      case SMALLINT => "castToShort"
      case INTEGER => "castToInt"
      case BIGINT => "castToLong"
      case FLOAT => "castToFloat"
      case DOUBLE => "castToDouble"
      case BOOLEAN => "castToBoolean"
      case _ => throw new CodeGenException(s"Unsupported decimal casting type: '$targetType'")
    }

    // no casting necessary
    if (isInteroperable(operandType, resultType)) {
      operandTerm => s"$operandTerm"
    }
    // decimal to decimal, may have different precision/scale
    else if (isDecimal(resultType) && isDecimal(operandType)) {
      val dt = resultType.asInstanceOf[DecimalType]
      operandTerm =>
        s"$DECIMAL_UTIL.castToDecimal($operandTerm, ${dt.getPrecision}, ${dt.getScale})"
    }
    // non_decimal_numeric to decimal
    else if (isDecimal(resultType) && isNumeric(operandType)) {
      val dt = resultType.asInstanceOf[DecimalType]
      operandTerm =>
        s"$DECIMAL_UTIL.castFrom($operandTerm, ${dt.getPrecision}, ${dt.getScale})"
    }
    // decimal to non_decimal_numeric
    else if (isNumeric(resultType) && isDecimal(operandType) ) {
      operandTerm =>
        s"$DECIMAL_UTIL.${decToPrimMethod(resultType)}($operandTerm)"
    }
    // numeric to numeric
    // TODO: Create a wrapper layer that handles type conversion between numeric.
    else if (isNumeric(operandType) && isNumeric(resultType)) {
      val resultTypeValue = resultTypeTerm + "Value()"
      val boxedTypeTerm = boxedTypeTermForType(operandType)
      operandTerm =>
        s"(new $boxedTypeTerm($operandTerm)).$resultTypeValue"
    }
    // result type is time interval and operand type is integer
    else if (isTimeInterval(resultType) && isInteger(operandType)){
      operandTerm => s"(($resultTypeTerm) $operandTerm)"
    }
    else {
      throw new CodeGenException(s"Unsupported casting from $operandType to $resultType.")
    }
  }

  private def localTimeToStringCode(
      ctx: CodeGeneratorContext,
      fromType: LogicalType,
      operandTerm: String): String =
    fromType.getTypeRoot match {
      case DATE =>
        s"${qualifyMethod(BuiltInMethod.UNIX_DATE_TO_STRING.method)}($operandTerm)"
      case TIME_WITHOUT_TIME_ZONE =>
        s"${qualifyMethod(BuiltInMethods.UNIX_TIME_TO_STRING)}($operandTerm)"
      case TIMESTAMP_WITHOUT_TIME_ZONE => // including rowtime indicator
        // The interpreted string conforms to the definition of timestamp literal
        // SQL 2011 Part 2 Section 6.13 General Rules 11) d)
        val precision = fromType.asInstanceOf[TimestampType].getPrecision
        s"${qualifyMethod(BuiltInMethods.TIMESTAMP_TO_STRING)}($operandTerm, $precision)"
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
        val method = qualifyMethod(BuiltInMethods.TIMESTAMP_TO_STRING_TIME_ZONE)
        val zone = ctx.addReusableSessionTimeZone()
        val precision = fromType.asInstanceOf[LocalZonedTimestampType].getPrecision
        s"$method($operandTerm, $zone, $precision)"
    }

}
