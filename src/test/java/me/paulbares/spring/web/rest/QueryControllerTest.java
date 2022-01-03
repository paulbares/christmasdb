package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.Query;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;

import java.util.List;
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
    mvc.perform(MockMvcRequestBuilders.post(QueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Object[] objects = JacksonUtil.mapper.readValue(contentAsString, Object[].class);
              Assertions.assertThat(objects).containsExactlyInAnyOrder(
                      List.of(MAIN_SCENARIO_NAME, 280.00000000000006d, 110.44985250737464),
                      List.of("mdd-baisse-simu-sensi", 190.00000000000003d, 102.94985250737463d),
                      List.of("mdd-baisse", 240.00000000000003d, 107.1165191740413d)
              );
            });
  }

  @Disabled("the controller does not used the toJson anymore")
  @Test
  public void testQueryDisabled() throws Exception {
    Query query = new Query()
            .addWildcardCoordinate("scenario")
            .addAggregatedMeasure("marge", "sum")
            .addExpressionMeasure("indice-prix", "100 * sum(`numerateur-indice`) / sum(`score-visi`)");
    mvc.perform(MockMvcRequestBuilders.post(QueryController.MAPPING_QUERY)
                    .content(JacksonUtil.serialize(query))
                    .contentType(MediaType.APPLICATION_JSON))
            .andExpect(status().isOk())
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Object[] objects = JacksonUtil.mapper.readValue(contentAsString, Object[].class);
              Assertions.assertThat(objects).containsExactlyInAnyOrder(
                      Map.of("scenario", MAIN_SCENARIO_NAME, "sum(marge)", 280.00000000000006d, "indice-prix",
                              110.44985250737464),
                      Map.of("scenario", "mdd-baisse-simu-sensi", "sum(marge)", 190.00000000000003d, "indice-prix",
                              102.94985250737463d),
                      Map.of("scenario", "mdd-baisse", "sum(marge)", 240.00000000000003d, "indice-prix",
                              107.1165191740413d)
              );
            });
  }

  @Test
  void testMetadata() throws Exception {
    mvc.perform(MockMvcRequestBuilders.get(QueryController.MAPPING_METADATA))
            .andExpect(result -> {
              String contentAsString = result.getResponse().getContentAsString();
              Object[] objects = JacksonUtil.mapper.readValue(contentAsString, Object[].class);
              Assertions.assertThat(objects).containsExactlyInAnyOrder(
                      Map.of("name", "ean", "type", "string"),
                      Map.of("name", "pdv", "type", "string"),
                      Map.of("name", "categorie", "type", "string"),
                      Map.of("name", "type-marque", "type", "string"),
                      Map.of("name", "sensibilite", "type", "string"),
                      Map.of("name", "quantite", "type", "int"),
                      Map.of("name", "prix", "type", "double"),
                      Map.of("name", "achat", "type", "int"),
                      Map.of("name", "score-visi", "type", "int"),
                      Map.of("name", "min-marche", "type", "double"),
                      Map.of("name", "ca", "type", "double"),
                      Map.of("name", "marge", "type", "double"),
                      Map.of("name", "numerateur-indice", "type", "double"),
                      Map.of("name", "indice-prix", "type", "double"),
                      Map.of("name", "scenario", "type", "string")
              );
            });
  }
}