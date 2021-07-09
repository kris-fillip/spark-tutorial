package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.collect_set

object Sindy {

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

    import spark.implicits._

    val data = inputs.map(filename =>
      spark.read
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ";")
        .csv(filename)
    )

    val cells = data
      .map(table =>
        table.flatMap(row => row
          .toSeq
          .zipWithIndex
          .map(value => (value._1.toString, row.schema.fieldNames(value._2))))
      )
      .reduce(_.union(_))

    val attributeSets = cells
      .groupBy("_1")
      .agg(collect_set("_2").as("columns"))
      .select("columns")
      .distinct()
      .as[Seq[String]]

    val inclusionList = attributeSets
      .flatMap(attributeSet => {
        attributeSet.map(col => (col, attributeSet.toSet - col))
      })

    val inclusionDependencies = inclusionList
      .groupByKey(_._1)
      .mapGroups((key, iterator) => (key, iterator.map(_._2)
      .reduce((intersected, newCandidate) => intersected.intersect(newCandidate))))
      .filter(_._2.nonEmpty)
      .sort("_1")
      .collect()

    inclusionDependencies
      .foreach(inclusionDependency =>
        println(inclusionDependency._1
          + " < "
          + inclusionDependency._2.toList.sorted.reduce(_ + "," + _)))
  }
}