package me.paulbares;

import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

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

  public static void main(String[] args) throws InterruptedException {
    var ean = new Field("Ean", String.class);
    var pdv = new Field("PDV", String.class);
    var categorie = new Field("Categorie", String.class);
    var type = new Field("Type_Marque", String.class);
    var sensi = new Field("Sensibilite", String.class);
    var quantite = new Field("Quantite", Integer.class);
    var prix = new Field("Prix", Double.class);
    var achat = new Field("Achat", Integer.class);
    var score = new Field("ScoreVisi", Integer.class);
    var minMarche = new Field("Min Marche", Double.class);

    Datastore datastore = new Datastore(
            List.of(ean, pdv, categorie, type, sensi, quantite, prix, achat, score, minMarche),
            quantite.col().multiply(prix.col()).as("CA"),
            quantite.col().multiply(prix.col().minus(achat.col())).as("Marge"),
            prix.col().divide(minMarche.col()).multiply(score.col()).as("NumerateurIndice"),
            col("NumerateurIndice").divide(score.col()).as("Indice prix"));

    datastore.load("Base", dataBase());
    datastore.load("MDD Baisse", dataMDDBaisse());
    datastore.load("MDD Baisse Simu Sensi", dataMDDBaisseSimuSensi());

//    datastore.get().show();

    datastore.get()
            .groupBy("Scenario", type.getName())
            .agg(sum(col("Marge")), sum(col("NumerateurIndice")), sum(score.col()))
            .withColumn("Indice Prix Visi",
                    col("sum(NumerateurIndice)").divide(col("sum(ScoreVisi)")).multiply(lit(100)))
            .show();

    datastore.get().createOrReplaceTempView("base_store");
    datastore.spark.sql("""
            SELECT Scenario, Type_Marque, sum(Marge), sum(NumerateurIndice), sum(ScoreVisi)
            FROM base_store
            group by Scenario, Type_Marque
            """).show();

//    Dataset<Row> select = datastore.get()
//            .groupBy("Scenario")
//            .agg(sum(col("Marge")), sum(col("NumerateurIndice")), sum(score.col()))
//            .withColumn("Indice Prix Visi",
//                    col("sum(NumerateurIndice)").divide(col("sum(ScoreVisi)")).multiply(lit(100)))
//            .select("Scenario", "sum(Marge)", "Indice Prix Visi");
//    select.show();
//
//    StructType schema = new StructType()
//            .add("Scenario", DataTypes.StringType)
//            .add("Group", DataTypes.StringType);
//
//    Map<String, List<String>> groups = new LinkedHashMap<>();
//    groups.put("A", List.of("Base", "MDD Baisse"));
//    groups.put("B", List.of("Base", "MDD Baisse Simu Sensi"));
//    groups.put("C", List.of("Base", "MDD Baisse", "MDD Baisse Simu Sensi"));
//
//    List<Row> rows = new ArrayList<>();
//    groups.forEach((group, subgroups) -> subgroups.forEach(scenario -> rows.add(RowFactory.create(scenario, group))));
//    Dataset<Row> dataFrame = datastore.spark.createDataFrame(rows, schema);// to load pojo
//    dataFrame.show();
//
//    Dataset<Row> join = select.join(dataFrame, select.col("Scenario").equalTo(dataFrame.col("Scenario")));
//    join.show();
//
//    WindowSpec window = Window.partitionBy("Group").orderBy(select.col("Scenario"));
//    join
//            .withColumn("delta(sum(Marge))", col("sum(Marge)").minus(lag("sum(Marge)", 1).over(window)))
//            .withColumn("delta(Indice Prix Visi)", col("Indice Prix Visi").minus(lag("Indice Prix Visi", 1).over(window)))
//            .select(dataFrame.col("Group"),dataFrame.col("Scenario"), col("delta(sum(Marge))"), col("delta(Indice Prix Visi)"))
//            .show();

//    Thread.currentThread().join();
  }
}