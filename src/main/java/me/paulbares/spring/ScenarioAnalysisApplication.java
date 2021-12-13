package me.paulbares.spring;

import me.paulbares.DataLoader;
import me.paulbares.Datastore;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.QueryEngine;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

@SpringBootApplication
public class ScenarioAnalysisApplication {

  public static void main(String[] args) {
    SpringApplication.run(ScenarioAnalysisApplication.class, args);
  }

  @Bean
  public QueryEngine queryEngine() {
    Datastore ds = DataLoader.createTestDatastoreWithData();
    return new QueryEngine(ds);
  }

  @Bean
  public MappingJackson2HttpMessageConverter jackson2HttpMessageConverter() {
    return new MappingJackson2HttpMessageConverter(JacksonUtil.mapper);
  }
}
