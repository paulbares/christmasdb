## Heroku CLI

```
heroku config:set JAVA_OPTS="-Xmx512m --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" -a sa-mvp
heroku config:set PORT="122021" -a sa-mvp

heroku logs --tail -a sa-mvp
```

## REST API

### DEV URL
- To check if the server is up and running, open: https://sa-mvp.herokuapp.com/ It can take 1 minute or so because it is hosted on Heroku and uses a free account that is turned off after a period of inactivity. Once up, a message will appear. 
- To execute a query, send a POST request to https://sa-mvp.herokuapp.com/spark-query. See payload example below. 
- To get the metadata of the store (to know the fields that can be queried): send a GET request to https://sa-mvp.herokuapp.com/spark-metadata. See response example below

### Specification

**Coordinates** are key/value pairs. 
Key refers to a field in the table. Possible values: ean, pdv, categorie, type-marque, sensibilite, quantite, prix, achat, score-visi, min-marche Value refers to the desired values for which the aggregates must be computed. It can be null (wildcard) to indicate all values must be returned or an array (of length >= 1) of possible values e.g "scenario": ["base", "mdd-baisse"].

**Measures** are either aggregated measures built from a field and an aggregation function (can be sum, min, max, avg) or calculated measures built from an sql expression (because Spark under the hood so it was the easiest way to do). Note the fields in the expression must be quoted with backticks (see example below).

#### Query payload example

Cossjoin of scenario|type-marque, measures are prix.sum, marge.sum and a calculated measure:

Payload:

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

This API can also be used for discovery! For instance to fetch all existing scenario:

Payload:
```json
{
  "coordinates": {
    "scenario": null
  }
}
```

```json
[
   {
      "scenario":"base"
   },
   {
      "scenario":"mdd-baisse-simu-sensi"
   },
   {
      "scenario":"mdd-baisse"
   }
]
```

#### Metadata response example

Response:
```json
[
    {
        "name": "ean",
        "type": "string"
    },
    {
        "name": "pdv",
        "type": "string"
    },
    {
        "name": "categorie",
        "type": "string"
    },
    {
        "name": "type-marque",
        "type": "string"
    },
    {
        "name": "sensibilite",
        "type": "string"
    },
    {
        "name": "quantite",
        "type": "int"
    },
    {
        "name": "prix",
        "type": "double"
    },
    {
        "name": "achat",
        "type": "int"
    },
    {
        "name": "score-visi",
        "type": "int"
    },
    {
        "name": "min-marche",
        "type": "double"
    },
    {
        "name": "ca",
        "type": "double"
    },
    {
        "name": "marge",
        "type": "double"
    },
    {
        "name": "numerateur-indice",
        "type": "double"
    },
    {
        "name": "indice-prix",
        "type": "double"
    },
    {
        "name": "scenario",
        "type": "string"
    }
]
```

