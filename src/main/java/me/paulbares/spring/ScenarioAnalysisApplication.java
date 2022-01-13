package me.paulbares.spring;

import me.paulbares.jackson.JacksonUtil;
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
  public MappingJackson2HttpMessageConverter jackson2HttpMessageConverter() {
    return new MappingJackson2HttpMessageConverter(JacksonUtil.mapper);
  }
}
