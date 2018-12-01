/*
 * FILE: functionTestScala.scala
 * Copyright (c) 2015 - 2018 GeoSpark Development Team
 *
 * MIT License
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package org.datasyslab.geosparksql

import com.vividsolutions.jts.geom.Geometry
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{IntegerType, StringType}

class functionTestScala extends TestBaseScala {

  describe("GeoSpark-SQL Function Test") {

    it("Passed ST_ConvexHull") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_ConvexHull(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Envelope") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Envelope(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Centroid") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Centroid(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Length") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Length(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Area") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Area(polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Distance") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Distance(polygondf.countyshape, polygondf.countyshape) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Transform") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Transform(polygondf.countyshape, 'epsg:4326','epsg:3857',true, false) from polygondf")
      functionDf.show()
    }

    it("Passed ST_Intersection - intersects but not contains") {

      val testtable=sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 8 1, 8 8, 1 8, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec=sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((2 8, 8 8, 8 2, 2 2, 2 8))"))
    }

    it("Passed ST_Intersection - intersects but left contains right") {

      val testtable=sparkSession.sql("select ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as a,ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec=sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((2 2, 2 3, 3 3, 2 2))"))
    }

    it("Passed ST_Intersection - intersects but right contains left") {

      val testtable=sparkSession.sql("select ST_GeomFromWKT('POLYGON((2 2, 2 3, 3 3, 2 2))') as a,ST_GeomFromWKT('POLYGON((1 1, 1 5, 5 5, 1 1))') as b")
      testtable.createOrReplaceTempView("testtable")
      val intersec=sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("POLYGON ((2 2, 2 3, 3 3, 2 2))"))
    }

    it("Passed ST_Intersection - not intersects") {

      var testtable=sparkSession.sql("select ST_GeomFromWKT('POLYGON((40 21, 40 22, 40 23, 40 21))') as a,ST_GeomFromWKT('POLYGON((2 2, 9 2, 9 9, 2 9, 2 2))') as b")
      testtable.createOrReplaceTempView("testtable")
      var intersec=sparkSession.sql("select ST_Intersection(a,b) from testtable")
      assert(intersec.take(1)(0).get(0).asInstanceOf[Geometry].toText.equals("MULTIPOLYGON EMPTY"))
    }


    it("Passed ST_IsValid") {

      var testtable=sparkSession.sql(
        "SELECT ST_IsValid(ST_GeomFromWKT('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (15 15, 15 20, 20 20, 20 15, 15 15))')) AS a, " +
        "ST_IsValid(ST_GeomFromWKT('POLYGON ((30 10, 40 40, 20 40, 10 20, 30 10))')) as b"
      )
      assert(!testtable.take(1)(0).get(0).asInstanceOf[Boolean])
      assert(testtable.take(1)(0).get(1).asInstanceOf[Boolean])
    }

    it("Fixed nullPointerException in ST_IsValid") {

      var testtable=sparkSession.sql(
        "SELECT ST_IsValid(null)"
      )
      assert(testtable.take(1).head.get(0) == null)
    }

    it("Passed ST_PrecisionReduce") {
      var testtable = sparkSession.sql(
        """
          |SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 8)
        """.stripMargin)
      testtable.show(false)
      assert(testtable.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinates()(0).x == 0.12345679)
      testtable = sparkSession.sql(
        """
          |SELECT ST_PrecisionReduce(ST_GeomFromWKT('Point(0.1234567890123456789 0.1234567890123456789)'), 11)
        """.stripMargin)
      testtable.show(false)
      assert(testtable.take(1)(0).get(0).asInstanceOf[Geometry].getCoordinates()(0).x == 0.12345678901)

    }

    it("Passed ST_Dimension LineString") {

      val testtable=sparkSession.sql("select ST_Dimension('GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))')")
      testtable.createOrReplaceTempView("testtable")
      assert(testtable.take(1)(0).get(0).asInstanceOf[Integer] == 1)
    }

    it("Passed ST_Dimension Point") {

      val testtable=sparkSession.sql("select ST_Dimension('GEOMETRYCOLLECTION(POINT(0 0))')")
      testtable.createOrReplaceTempView("testtable")
      assert(testtable.take(1)(0).get(0).asInstanceOf[Integer] == 0)
    }

    it("Passed ST_Dimension Polygon") {

      val testtable=sparkSession.sql("select ST_Dimension('GEOMETRYCOLLECTION(POLYGON(0 0, 0 1, 1 0, 1 1), ,POINT(0 0))')")
      testtable.createOrReplaceTempView("testtable")
      assert(testtable.take(1)(0).get(0).asInstanceOf[Integer] == 2)
    }


    it("Passed ST_GeometryType") {

      val testtable=sparkSession.sql("select ST_GeometryType(ST_GeomFromText('LINESTRING(77.29 29.07,77.42 29.26,77.27 29.31,77.29 29.07)'))")
      testtable.createOrReplaceTempView("testtable")
    }

    it("Passed ST_AsText") {
      val testtable=sparkSession.sql("SELECT ST_AsText(ST_GeomFromWKT('POINT(111.1111111 1.1111111)'))")
      testtable.createOrReplaceTempView("testtable")
    }

    it("Passed ST_X") {
      var testtable=sparkSession.sql(
        "SELECT ST_X(ST_GeomFromWKT('POINT(1  2)'))"
      )
      assert(testtable.take(1)(0).get(0).asInstanceOf[Double] == 1.0)
    }

    it("Passed ST_Y") {
      var testtable=sparkSession.sql(
        "SELECT ST_Y(ST_GeomFromWKT('POINT(1  2)'))"
      )
      assert(testtable.take(1)(0).get(0).asInstanceOf[Double] == 2.0)
    }

    it("Passed Centroid ST_Y") {
      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_GeomFromWKT(polygontable._c0) as countyshape from polygontable")
      polygonDf.createOrReplaceTempView("polygondf")
      polygonDf.show()
      var functionDf = sparkSession.sql("select ST_Y(ST_Centroid(polygondf.countyshape)) from polygondf")
      assert(functionDf.take(1)(0).get(0).asInstanceOf[Double] == 41.916402825970664)
    }

    it("Passed ST_IsSimple") {
      var testtable=sparkSession.sql(
        "SELECT ST_IsSimple(ST_GeomFromText('LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)'))"
      )
      assert(!testtable.take(1)(0).get(0).asInstanceOf[Boolean])
    }

    it("Passed ST_IsEmpty") {
      var testDF = sparkSession.sql("select ST_IsEmpty(ST_GeomFromText('GEOMETRYCOLLECTION EMPTY'))")
      assert(testDF.take(1)(0).get(0).asInstanceOf[Boolean])

      var polygonWktDf = sparkSession.read.format("csv").option("delimiter", "\t").option("header", "false").load(mixedWktGeometryInputLocation)
      polygonWktDf.createOrReplaceTempView("polygontable")
      polygonWktDf.show()
      var polygonDf = sparkSession.sql("select ST_IsEmpty(ST_GeomFromText(polygontable._c0)) as status from polygontable")
      polygonDf.show(10)
      assert(polygonDf.count() == 100)
      polygonDf.createOrReplaceTempView("polygontable")
      var sampletestDf = sparkSession.sql("select count(polygontable.status) from polygontable where polygontable.status = 'true'")
      assert(sampletestDf.take(1)(0).get(0).asInstanceOf[Long] == 0)
    }

    it("Passed ST_IsClosed") {

      var testtable=sparkSession.sql(
        "SELECT ST_IsClosed(ST_GeomFromWKT('LINESTRING(0 0, 1 1))'))"
      )
      assert(!testtable.take(1)(0).get(0).asInstanceOf[Boolean])


      testtable=sparkSession.sql(
        "SELECT ST_IsClosed(ST_GeomFromWKT('LINESTRING(0 0, 0 1, 1 1, 0 0)'))"
      )
      assert(testtable.take(1)(0).get(0).asInstanceOf[Boolean])

      testtable=sparkSession.sql(
        "SELECT ST_IsClosed(ST_GeomFromWKT('MULTILINESTRING((0 0, 0 1, 1 1, 0 0),(0 0, 1 1))'))"
      )
      assert(!testtable.take(1)(0).get(0).asInstanceOf[Boolean])

      testtable=sparkSession.sql(
        "SELECT ST_IsClosed(ST_GeomFromWKT('POINT(0 0)'))"
      )
      assert(testtable.take(1)(0).get(0).asInstanceOf[Boolean])

      testtable=sparkSession.sql(
        "SELECT ST_IsClosed(ST_GeomFromWKT('MULTIPOINT((0 0), (1 1))'))"
      )
      assert(testtable.take(1)(0).get(0).asInstanceOf[Boolean])

    }
  }
}
