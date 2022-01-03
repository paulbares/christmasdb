package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.Query;
import me.paulbares.query.QueryEngine;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class QueryController {

  public static final String MAPPING_QUERY = "/spark-query";

  public static final String MAPPING_METADATA = "/spark-metadata";

  protected final QueryEngine queryEngine;

  public QueryController(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<String> execute(@RequestBody Query query) {
    Dataset<Row> rowDataset = this.queryEngine.executeSparkSql(query);
    return ResponseEntity.ok(JacksonUtil.datasetToCsv(rowDataset));
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<List<Map<String, String>>> getMetadata() {
    List<Map<String, String>> collect = Arrays.stream(this.queryEngine.datastore.getFields())
            .map(f -> Map.of("name", f.name(), "type", f.dataType().simpleString()))
            .collect(Collectors.toCollection(() -> new ArrayList<>()));
    collect.add(Map.of("name", "scenario", "type", DataTypes.StringType.simpleString()));
    return ResponseEntity.ok(collect);
  }
}
