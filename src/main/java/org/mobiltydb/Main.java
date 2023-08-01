package org.mobiltydb;

import org.apache.spark.sql.*;
import org.mobiltydb.UDT.classes.TGeomPointInst;

import static jmeos.functions.functions.*;
import static jmeos.functions.functions_old.meos_finalize;

import jmeos.functions.functions_old;
import jmeos.types.basic.tpoint.TPoint;
import jmeos.types.temporal.TemporalValue;

public class Main {
    public static void main(String[] args) {
        meos_initialize("UTC");

        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        // Create an array of TGeomPointInst instances
        TGeomPointInst[] pointsArray = new TGeomPointInst[]{
                new TGeomPointInst(java.sql.Timestamp.valueOf("2023-07-20 12:00:00"), 10.5),
                new TGeomPointInst(java.sql.Timestamp.valueOf("2023-07-21 15:30:00"), 15.3),
                new TGeomPointInst(java.sql.Timestamp.valueOf("2023-07-22 18:45:00"), 20.1)
        };

        // Convert the array to a Dataset
        Dataset<Row> pointsDF = spark.createDataFrame(java.util.Arrays.asList(pointsArray), TGeomPointInst.class);

        // Show the DataFrame
        pointsDF.show();

        // Perform some basic operations on the DataFrame
        Dataset<Row> filteredPoints = pointsDF.filter("value > 15.0");
        filteredPoints.show();

        // Stop the Spark session
        spark.stop();
        meos_finalize();
    }
}
