package com.datastax.spark.connector

import com.datastax.driver.core.{CodecRegistry, ResultSet, Row, TypeCodec}

/** Represents a single row fetched from Cassandra.
  * Offers getters to read individual fields by column name or column index.
  * The getters try to convert value to desired type, whenever possible.
  * Most of the column types can be converted to a `String`.
  * For nullable columns, you should use the `getXXXOption` getters which convert
  * `null`s to `None` values, otherwise a `NullPointerException` would be thrown.
  *
  * All getters throw an exception if column name/index is not found.
  * Column indexes start at 0.
  *
  * If the value cannot be converted to desired type,
  * [[com.datastax.spark.connector.types.TypeConversionException]] is thrown.
  *
  * Recommended getters for Cassandra types:
  *
  *   - `ascii`:     `getString`, `getStringOption`
  *   - `bigint`:    `getLong`, `getLongOption`
  *   - `blob`:      `getBytes`, `getBytesOption`
  *   - `boolean`:   `getBool`, `getBoolOption`
  *   - `counter`:   `getLong`, `getLongOption`
  *   - `decimal`:   `getDecimal`, `getDecimalOption`
  *   - `double`:    `getDouble`, `getDoubleOption`
  *   - `float`:     `getFloat`, `getFloatOption`
  *   - `inet`:      `getInet`, `getInetOption`
  *   - `int`:       `getInt`, `getIntOption`
  *   - `text`:      `getString`, `getStringOption`
  *   - `timestamp`: `getDate`, `getDateOption`
  *   - `timeuuid`:  `getUUID`, `getUUIDOption`
  *   - `uuid`:      `getUUID`, `getUUIDOption`
  *   - `varchar`:   `getString`, `getStringOption`
  *   - `varint`:    `getVarInt`, `getVarIntOption`
  *   - `list`:      `getList[T]`
  *   - `set`:       `getSet[T]`
  *   - `map`:       `getMap[K, V]`
  *
  * Collection getters `getList`, `getSet` and `getMap` require to explicitly pass an appropriate item type:
  * {{{
  * row.getList[String]("a_list")
  * row.getList[Int]("a_list")
  * row.getMap[Int, String]("a_map")
  * }}}
  *
  * Generic `get` allows to automatically convert collections to other collection types.
  * Supported containers:
  *   - `scala.collection.immutable.List`
  *   - `scala.collection.immutable.Set`
  *   - `scala.collection.immutable.TreeSet`
  *   - `scala.collection.immutable.Vector`
  *   - `scala.collection.immutable.Map`
  *   - `scala.collection.immutable.TreeMap`
  *   - `scala.collection.Iterable`
  *   - `scala.collection.IndexedSeq`
  *   - `java.util.ArrayList`
  *   - `java.util.HashSet`
  *   - `java.util.HashMap`
  *
  * Example:
  * {{{
  * row.get[List[Int]]("a_list")
  * row.get[Vector[Int]]("a_list")
  * row.get[java.util.ArrayList[Int]]("a_list")
  * row.get[TreeMap[Int, String]]("a_map")
  * }}}
  *
  *
  * Timestamps can be converted to other Date types by using generic `get`. Supported date types:
  *   - java.util.Date
  *   - java.sql.Date
  *   - org.joda.time.DateTime
  */
final class CassandraRow(val metaData: CassandraRowMetaData, val columnValues: IndexedSeq[AnyRef])
  extends ScalaGettableData with Serializable {

  /**
    * The constructor is for testing and backward compatibility only.
    * Use default constructor with shared metadata for memory saving and performance.
    *
    * @param columnNames
    * @param columnValues
    */
  @deprecated
  def this (columnNames: IndexedSeq[String], columnValues: IndexedSeq[AnyRef]) =
    this(CassandraRowMetaData.fromColumnNames(columnNames), columnValues)

  override def toString = "CassandraRow" + dataAsString
}

case class CassandraRowMetaData(columnNames: IndexedSeq[String], resultSetColumnNames:Option[IndexedSeq[String]] = None, @transient codecs: Option[IndexedSeq[TypeCodec[AnyRef]]] = None) {
  @transient
  lazy val namesToIndex: Map[String,Int] = columnNames.zipWithIndex.toMap.withDefaultValue(-1);
  @transient
  lazy val indexOfCqlColumnOrThrow = unaliasColumnNames.zipWithIndex.toMap.withDefault { name =>
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")
  }

  @transient
  lazy val indexOfOrThrow = namesToIndex.withDefault { name =>
    throw new ColumnNotFoundException(
      s"Column not found: $name. " +
        s"Available columns are: ${columnNames.mkString("[", ", ", "]")}")
  }
  def unaliasColumnNames = resultSetColumnNames.getOrElse(columnNames)
}

object CassandraRowMetaData {

  def fromResultSet(columnNames:IndexedSeq[String], rs: ResultSet) = {
    import scala.collection.JavaConversions._
    val columnDefs = rs.getColumnDefinitions.asList().toList
    val rsColumnNames = columnDefs.map(_.getName)
    val codecs = columnDefs.map( col => CodecRegistry.DEFAULT_INSTANCE.codecFor(col.getType))
      .asInstanceOf[List[TypeCodec[AnyRef]]]
    CassandraRowMetaData (columnNames, Some(rsColumnNames.toIndexedSeq), Some(codecs.toIndexedSeq))
  }


  /**
    * create metadata object without codecs. Should be used for testing only
    *
    * @param columnNames
    * @return
    */
  def fromColumnNames (columnNames: IndexedSeq[String]): CassandraRowMetaData =
    CassandraRowMetaData(columnNames, None)

  def fromColumnNames (columnNames: Seq[String]): CassandraRowMetaData = fromColumnNames(columnNames.toIndexedSeq)
}

object CassandraRow {

  /** Deserializes first n columns from the given `Row` and returns them as
    * a `CassandraRow` object. The number of columns retrieved is determined by the length
    * of the columnNames argument. The columnNames argument is used as metadata for
    * the newly created `CassandraRow`, but it is not used to fetch data from
    * the input `Row` in order to improve performance. Fetching column values by name is much
    * slower than fetching by index. */
  def fromJavaDriverRow(row: Row, metaData: CassandraRowMetaData): CassandraRow = {
    val data = new Array[Object](metaData.columnNames.length)
    for (i <- metaData.columnNames.indices) {
      data(i) = metaData.codecs match {
        case Some(codecs) => GettableData.get (row, i, metaData.codecs.get (i) )
        case None => GettableData.get (row, i)
      }
    }
    new CassandraRow(metaData, data)
  }


  /** Creates a CassandraRow object from a map with keys denoting column names and
    * values denoting column values. */
  def fromMap(map: Map[String, Any]): CassandraRow = {
    val (columnNames, values) = map.unzip
    new CassandraRow(CassandraRowMetaData.fromColumnNames(columnNames.toIndexedSeq), values.map(_.asInstanceOf[AnyRef]).toIndexedSeq)
  }

}
