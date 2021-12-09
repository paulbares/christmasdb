package me.paulbares

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions

data class Field(val name: String, val type: Class<out Any>) {

    fun col(): Column {
        return functions.col(name)
    }

    operator fun plus(other: Field): Field {
        return Field(name + "+" + other.name, javaClass)
    }

    operator fun div(other: Field): Field {
        return Field(name + "/" + other.name, javaClass)
    }

    operator fun minus(other: Field): Field {
        return Field(name + "-" + other.name, javaClass)
    }

    operator fun times(other: Field): Field {
        return Field(name + "*" + other.name, javaClass)
    }
}

