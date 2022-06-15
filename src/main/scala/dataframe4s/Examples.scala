package dataframe4s

@main
def examples(): Unit = {
  val people: DataFrame[
    (
        Field["name", String],
        Field["surname", String],
        Field["age", Int],
        Field["is_programmer", Boolean]
    )
  ] =
    DataFrame.select
      .withColumn("name", lit("Vitalii"))
      .withColumn("surname", lit("Honta"))
      .withColumn("age", lit(24))
      .withColumn("is_programmer", lit(true))

  //
  // Uncomment to check compilation errors.
  // It would be something like
  //  [error] -- [E007] Type Mismatch Error: Examples.scala:20:17
  //   [error] 20 |    people.select(people("non_existing"))
  //   [error]    |    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  //   [error]    |Found:    dataframe4s.DataFrame[dataframe4s.ColumnNotFoundError[("non_existing" : String)]
  //   [error]    |
  //   [error]    |*: EmptyTuple]
  //   [error]    |Required: dataframe4s.DataFrame[
  //   [error]    |  dataframe4s.One[dataframe4s.Field[("non_existing" : String), String]]
  //   [error]    |]
  //
  // val x: DataFrame[One[Field["non_existing", String]]] =
  // people.select(people("non_existing"))

  val names: DataFrame[(Field["name", String], Field["age", Int])] =
    people
      .select(
        people("name") &
          people("age")
      )
      .where(people("age") > lit(20))
}
