package me.paulbares;

import java.util.List;

import static org.apache.spark.sql.functions.col;

/**
 * --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 */
public class DataLoader {

  static List<String> headers() {
    return List.of("ean", "pdv", "categorie", "type marque", "sensibilite", "quantite", "prix", "achat", "score",
            "min marche");
  }

  static List<Object[]> dataBase() {
    return List.of(
            new Object[]{"Nutella 500g", "Toulouse Centre", "Pate Noisette", "MN", "Hyper", 100, 5.9d, 5, 100, 5.65d},
            new Object[]{"ChocoNoisette 500g", "Toulouse Centre", "Pate Noisette", "MDD", "Hyper", 100, 4.9d, 3, 50, 4d}
    );
  }

  static List<Object[]> dataMDDBaisse() {
    return List.of(
            new Object[]{"Nutella 500g", "Toulouse Centre", "Pate Noisette", "MN", "Hyper", 100, 5.9d, 5, 100, 5.65d},
            new Object[]{"ChocoNoisette 500g", "Toulouse Centre", "Pate Noisette", "MDD", "Hyper", 100, 4.5d, 3, 50, 4d}
    );
  }

  static List<Object[]> dataMDDBaisseSimuSensi() {
    return List.of(
            new Object[]{"Nutella 500g", "Toulouse Centre", "Pate Noisette", "MN", "Hyper", 100, 5.9d, 5, 100, 5.65d},
            new Object[]{"ChocoNoisette 500g", "Toulouse Centre", "Pate Noisette", "MDD", "Basse", 100, 4d, 3, 50, 4d}
    );
  }

  public static SparkDatastore createTestDatastoreWithData() {
    var ean = new CustomField("ean", String.class);
    var pdv = new CustomField("pdv", String.class);
    var categorie = new CustomField("categorie", String.class);
    var type = new CustomField("type-marque", String.class);
    var sensi = new CustomField("sensibilite", String.class);
    var quantite = new CustomField("quantite", Integer.class);
    var prix = new CustomField("prix", Double.class);
    var achat = new CustomField("achat", Integer.class);
    var score = new CustomField("score-visi", Integer.class);
    var minMarche = new CustomField("min-marche", Double.class);

    SparkDatastore datastore = new SparkDatastore(
            List.of(ean, pdv, categorie, type, sensi, quantite, prix, achat, score, minMarche),
            quantite.col().multiply(prix.col()).as("ca"),
            quantite.col().multiply(prix.col().minus(achat.col())).as("marge"),
            prix.col().divide(minMarche.col()).multiply(score.col()).as("numerateur-indice"),
            col("numerateur-indice").divide(score.col()).as("indice-prix"));

    datastore.load(SparkDatastore.MAIN_SCENARIO_NAME, dataBase());
    datastore.load("mdd-baisse", dataMDDBaisse());
    datastore.load("mdd-baisse-simu-sensi", dataMDDBaisseSimuSensi());

    return datastore;
  }
}