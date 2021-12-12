package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.Query;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest
@AutoConfigureMockMvc
public class QueryControllerTest {

  @Autowired
  private MockMvc mvc;

  @Test
  public void testQuery() throws Exception {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("marge", "sum")
            .addAggregatedMeasure("numerateur-indice", "sum")
            .addAggregatedMeasure("score-visi", "sum");
    mvc.perform(MockMvcRequestBuilders.post(QueryController.MAPPING)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(new ResultMatcher() {
              @Override
              public void match(MvcResult result) throws Exception {
                System.out.println();
              }
            });
  }
}