## Heroku CLI

```
heroku config:set JAVA_OPTS="-Xmx512m --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" -a sa-mvp
heroku config:set PORT="122021" -a sa-mvp

heroku logs --tail -a sa-mvp
```

## REST API

### Specification

**Coordinates** are key/value pairs. 
Key refers to a field in the table. Possible values: `ean, pdv, categorie, type-marque, sensibilite, quantite, prix, achat, score-visi, min-marche`
Value refers to the desired values for which the aggregates must be computed. It can be `null` to indicate all values must be returned or an array of possible values e.g "scenario": ["base", "mdd-baisse"].

**Measures** are either aggregated measures built from a field and an aggregation function (can be `sum, min, max, avg`) or calculated measures built from an sql expression (because Spark under the hood so it was the easiest way to do). Note the fields in the expression must be quoted with backticks (see example below).   

Query, crossjoin of scenario|type-marque, measures are prix.sum, marge.sum and a calculated measure:
```json
{
  "coordinates": {
    "scenario": null,
    "type-marque": null
  },
  "measures": [
    {
      "field": "prix",
      "aggregationFunction": "sum"
    },
    {
      "field": "marge",
      "aggregationFunction": "sum"
    },
    {
      "alias": "indice-prix",
      "expression": "100 * sum(`numerateur-indice`) / sum(`score-visi`)"
    }
  ]
}
```
Reponse:
```json
[
    {
        "scenario": "base",
        "type-marque": "MDD",
        "sum(prix)": 4.9,
        "sum(marge)": 190.00000000000003,
        "indice-prix": 122.50000000000001
    },
    {
        "scenario": "base",
        "type-marque": "MN",
        "sum(prix)": 5.9,
        "sum(marge)": 90.00000000000003,
        "indice-prix": 104.42477876106196
    },
    {
        "scenario": "mdd-baisse-simu-sensi",
        "type-marque": "MDD",
        "sum(prix)": 4.0,
        "sum(marge)": 100.0,
        "indice-prix": 100.0
    },
    {
        "scenario": "mdd-baisse-simu-sensi",
        "type-marque": "MN",
        "sum(prix)": 5.9,
        "sum(marge)": 90.00000000000003,
        "indice-prix": 104.42477876106196
    },
    {
        "scenario": "mdd-baisse",
        "type-marque": "MN",
        "sum(prix)": 5.9,
        "sum(marge)": 90.00000000000003,
        "indice-prix": 104.42477876106196
    },
    {
        "scenario": "mdd-baisse",
        "type-marque": "MDD",
        "sum(prix)": 4.5,
        "sum(marge)": 150.0,
        "indice-prix": 112.5
    }
]
```
