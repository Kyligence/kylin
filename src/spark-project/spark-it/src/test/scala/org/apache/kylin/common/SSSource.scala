/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

package org.apache.kylin.common

import java.io.{DataInputStream, File}
import java.nio.charset.Charset
import java.nio.file.Files
import java.util.Locale

import org.apache.commons.io.IOUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kylin.common.persistence.{JsonSerializer, RootPersistentEntity}
import org.apache.kylin.common.util.TempMetadataBuilder
import org.apache.kylin.guava30.shaded.common.base.Preconditions
import org.apache.kylin.metadata.cube.model.{IndexPlan, NDataflowManager, NIndexPlanManager}
import org.apache.kylin.metadata.model.{NDataModel, NDataModelManager, NTableMetadataManager}
import org.apache.kylin.metadata.project.NProjectManager
import org.apache.kylin.query.util.{PushDownUtil, QueryParams}
import org.apache.spark.sql.SparderEnv
import org.apache.spark.sql.common.{LocalMetadata, SharedSparkSession}
import org.apache.spark.sql.execution.utils.SchemaProcessor
import org.apache.spark.sql.udf.UdfManager
import org.scalatest.Suite

trait SSSource extends SharedSparkSession with LocalMetadata {
  self: Suite =>

  val CSV_TABLE_DIR: String = "../" + TempMetadataBuilder.TEMP_TEST_METADATA + "/data/%s.csv"

  override def beforeAll(): Unit = {
    super.beforeAll()
    UdfManager.create(spark)
    SparderEnv.setSparkSession(spark)
    val project = getProject
    import org.apache.kylin.metadata.project.NProjectManager
    val kylinConf = KylinConfig.getInstanceFromEnv
    val projectInstance =
      NProjectManager.getInstance(kylinConf).getProject(project)
    Preconditions.checkArgument(projectInstance != null)
    import scala.collection.JavaConverters._
    projectInstance.getTables.asScala
      .filter(!_.equals("DEFAULT.STREAMING_TABLE"))
      .foreach { table =>
        val tableDesc = NTableMetadataManager
          .getInstance(kylinConf, project)
          .getTableDesc(table)
        val columns = tableDesc.getColumns
        val schema = SchemaProcessor.buildSchemaWithRawTable(columns)
        var tableN = tableDesc.getName
        if (table.equals("DEFAULT.TEST_KYLIN_FACT")) {
          tableN = tableDesc.getName + "_table"
        }
        spark.catalog.createTable(
          tableName = tableN,
          source = "csv",
          schema = schema,
          options = Map("path" -> String.format(Locale.ROOT, CSV_TABLE_DIR, table)))
        if (table.equals("DEFAULT.TEST_KYLIN_FACT")) {
          spark.sql("create view " + tableDesc.getName + " as select * from " + tableN)
        }
      }
  }

  protected def getProject: String = "default"

  def cleanSql(originSql: String): String = {
    val sqlForSpark = originSql
      .replaceAll("edw\\.", "")
      .replaceAll("\"EDW\"\\.", "")
      .replaceAll("EDW\\.", "")
      .replaceAll("default\\.", "")
      .replaceAll("DEFAULT\\.", "")
      .replaceAll("\"DEFAULT\"\\.", "")
    val queryParams = new QueryParams("default", sqlForSpark, "DEFAULT", false)
    queryParams.setKylinConfig(NProjectManager.getProjectConfig("default"))
    PushDownUtil.massagePushDownSql(queryParams)
  }

  def addModels(resourcePath: String, modelIds: Seq[String]): Unit = {
    val modelMgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    val indexPlanMgr = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    val dfMgr = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv, getProject)
    modelIds.foreach { id =>
      val model = read(classOf[NDataModel], resourcePath, s"model_desc/$id.json")
      model.setProject(getProject)
      modelMgr.createDataModelDesc(model, "ADMIN")
      dfMgr.createDataflow(indexPlanMgr.createIndexPlan(
        read(classOf[IndexPlan], resourcePath, s"index_plan/$id.json")), "ADMIN")
    }
  }

  private def read[T <: RootPersistentEntity](clz: Class[T], prePath: String, subPath: String): T = {
    val path = prePath + subPath;
    // val path = "src/test/resources/view/" + subPath
    val contents = StringUtils.join(Files.readAllLines(new File(path).toPath, Charset.defaultCharset), "\n")
    val bais = IOUtils.toInputStream(contents, Charset.defaultCharset)
    new JsonSerializer[T](clz).deserialize(new DataInputStream(bais))
  }
}
