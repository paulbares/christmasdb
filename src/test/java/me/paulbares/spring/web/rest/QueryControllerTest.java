package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.Query;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.Map;

import static me.paulbares.Datastore.MAIN_SCENARIO_NAME;
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
            .addExpressionMeasure("indice-prix", "100 * sum(`numerateur-indice`) / sum(`score-visi`)");
    mvc.perform(MockMvcRequestBuilders.post(QueryController.MAPPING)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
				String contentAsString = result.getResponse().getContentAsString();
				Object[] objects = JacksonUtil.mapper.readValue(contentAsString, Object[].class);
				Assertions.assertThat(objects).containsExactlyInAnyOrder(
								Map.of("scenario", MAIN_SCENARIO_NAME, "sum(marge)", 280.00000000000006d, "indice-prix", 110.44985250737464),
								Map.of("scenario", "mdd-baisse-simu-sensi", "sum(marge)", 190.00000000000003d, "indice-prix", 102.94985250737463d),
								Map.of("scenario", "mdd-baisse", "sum(marge)", 240.00000000000003d, "indice-prix", 107.1165191740413d)
				);
			});
  }
}