package me.paulbares;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Datastore {

  private final Map<String, Dataset<Row>> m = new HashMap<>();

  final StructType schema;

  final SparkSession spark;

  private Column[] columns;

  public Datastore(List<Field> fields, Column... columns) {
    this.schema = createSchema(fields.toArray(new Field[0]));
    this.spark = SparkSession
            .builder()
            .appName("Java Spark SQL Example")
            .config("spark.master", "local")
            .getOrCreate();
    this.columns = columns;
  }

  public void load(String scenario, List<Object[]> tuples) {
    List<Row> rows = tuples.stream().map(RowFactory::create).toList();
    Dataset<Row> dataFrame = this.spark.createDataFrame(rows, schema);// to load pojo
    for (Column column : columns) {
      dataFrame = dataFrame.withColumn(column.named().name(), column);
    }
    Dataset<Row> previous = this.m.putIfAbsent(scenario, dataFrame);
    if (previous != null) {
      throw new RuntimeException("Already existing dataset for scenario " + scenario);
    }
  }

  public void show(String scenario) {
    this.m.get(scenario).withColumn("Scenario", functions.lit(scenario)).show(100);
  }

  public Dataset<Row> get() {
    List<Dataset<Row>> list = new ArrayList<>();
    Dataset<Row> union = null;
    for (Map.Entry<String, Dataset<Row>> e : this.m.entrySet()) {
      if (e.getKey().equals("Base")) {
        union = e.getValue().withColumn("Scenario", functions.lit(e.getKey()));
        for (Dataset<Row> d : list) {
          union = union.unionAll(d);
        }
      } else {
        Dataset<Row> scenario = e.getValue().withColumn("Scenario", functions.lit(e.getKey()));
        if (union == null) {
          list.add(scenario);
        } else {
          union = union.unionAll(scenario);
        }
      }
    }
    return union;
  }

  private static StructType createSchema(Field... fields) {
    StructType schema = new StructType();
    for (Field field : fields) {
      DataType type;
      if (field.getType().equals(String.class)) {
        type = DataTypes.StringType;
      } else if (field.getType().equals(Double.class)) {
        type = DataTypes.DoubleType;
      } else if (field.getType().equals(Integer.class)) {
        type = DataTypes.IntegerType;
      } else {
        throw new RuntimeException();
      }
      schema = schema.add(field.getName(), type);
    }
    return schema;
  }
}
