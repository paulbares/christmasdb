package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.Query;
import me.paulbares.query.QueryEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QueryController {

  public static final String MAPPING = "/spark-query";

  protected final QueryEngine queryEngine;

  public QueryController(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  @PostMapping(MAPPING)
  public ResponseEntity<String> execute(@RequestBody Query query) {
    Dataset<Row> rowDataset = this.queryEngine.executeSparkSql(query);
    return ResponseEntity.ok(JacksonUtil.datasetToJSON(rowDataset));
  }
}
