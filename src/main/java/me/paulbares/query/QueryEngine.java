package me.paulbares.query;

import me.paulbares.Datastore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.logging.Logger;

public class QueryEngine {

  private static final Logger LOGGER = Logger.getLogger(QueryEngine.class.getName());

  private final Datastore datastore;

  public QueryEngine(Datastore datastore) {
    this.datastore = datastore;
  }

  public Dataset<Row> executeSparkSql(Query query) {
    LOGGER.info("Executing " + query);
    String sql = SQLTranslator.translate(query);
    LOGGER.info("Translated query #" + query.id + " to " + sql);
    datastore.get().createOrReplaceTempView(Datastore.BASE_STORE_NAME);
    return datastore.spark.sql(sql);
  }
}
