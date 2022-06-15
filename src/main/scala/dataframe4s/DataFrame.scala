package dataframe4s

// alias for single tuple
type One[+A] = A *: EmptyTuple

// Captures field name as a type parameter
sealed trait Field[N <: String & Singleton, A]

/** TLDR; A lot of tuple magic You can skip
  */

//
//  Generic tuple functions
//

// Remove element E from tuple L
type Remove[L <: Tuple, E] <: Tuple = L match {
  case EmptyTuple => EmptyTuple
  case E *: ls    => Remove[ls, E]
  case l *: ls    => l *: Remove[ls, E]
}

// Ensures tuple T contains element of type E
type Contains[T <: Tuple, E] <: Boolean = T match {
  case EmptyTuple => false
  case E *: _     => true
  case _ *: xs    => Contains[xs, E]
}

// Boolean Not on types
type Not[P <: Boolean] <: Boolean = P match {
  case false => true
  case true  => false
}

// Appends element to the end of the tuple
type AppendToTuple[X <: Tuple, Y] <: Tuple = X match {
  case EmptyTuple => Y *: EmptyTuple
  case x *: xs    => x *: AppendToTuple[xs, Y]
}

// Tuple.Concat without duplicates
type UniqueUnion[L <: Tuple, R <: Tuple] =
  Tuple.Concat[L, Tuple.Filter[R, [x] =>> Not[Contains[L, x]]]]

//
//  DataFrame specific tuple functions
//

// Merge Expression outputs so that it becomes a tuple
type MergeExpressionResults[L, R] <: Tuple = (L, R) match {
  case (EmptyTuple, EmptyTuple) => EmptyTuple
  case (l *: ls, EmptyTuple)    => l *: ls
  case (EmptyTuple, r *: rs)    => r *: rs
  case (l *: ls, r *: rs)       => UniqueUnion[l *: ls, r *: rs]
  case (l *: ls, _)             => AppendToTuple[l *: ls, R]
  case (_, r *: rs)             => L *: r *: rs
  case _                        => L *: R *: EmptyTuple
}

// Ensures select result is a tuple
type SelectTuple[F <: Tuple, A] <: Tuple = F match {
  case EmptyTuple                => EmptyTuple
  case Field[n, A] *: EmptyTuple => One[Field[n, A]]
  case Field[n, v] *: xs =>
    A match {
      case EmptyTuple => EmptyTuple
      case v *: ys    => Field[n, v] *: SelectTuple[xs, ys]
    }
}

// Typelevel assertion on tuples
type Assert[
    T <: Tuple, // Input
    P[_] <: Boolean, // Predicate
    Success <: Tuple, // On success
    Fail[_] <: Tuple // On failure
] <: Tuple = T match {
  case EmptyTuple => Success
  case h *: t =>
    P[h] match {
      case false => Fail[h]
      case true  => Assert[t, P, Success, Fail]
    }
}

// Helper typelevel error
sealed trait ColumnNotFoundError[N <: String & Singleton] extends Any
type ColumnNotFound[A] <: Tuple = A match {
  case Field[n, _] => ColumnNotFoundError[n] *: EmptyTuple
}

// Ensures tuple R contains the same elements as tuple L
type AllColumnsExist[
    L <: Tuple,
    R <: Tuple,
    OnEmpty <: Tuple, // What to do if L is empty
    OnSuccess <: Tuple // What to do if everything is OK
] <: Tuple = L match {
  case EmptyTuple => OnEmpty
  case _ =>
    Assert[R, [x] =>> Contains[L, x], OnSuccess, [x] =>> ColumnNotFound[x]]
}

// Summons type of field N from schema L
type FindTypeOf[L <: Tuple, N <: String & Singleton] = L match {
  case EmptyTuple       => ColumnNotFoundError[N]
  case Field[N, a] *: _ => a
  case _ *: xs          => FindTypeOf[xs, N]
}

// helper to capture field name
class ColumnPartiallyApplied[A](private val `dummy`: Boolean = true)
    extends AnyVal {
  def apply[N <: String & Singleton](
      name: N
  ): One[Field[N, A]] = ???
}

// Allows to reference the column
def col[A]: ColumnPartiallyApplied[A] = new ColumnPartiallyApplied[A]()

// Typed dataframe expression
trait Expression[In <: Tuple, Out] {
  // Comparison TODO: use Ordering
  def >[In0 <: Tuple](
      that: Expression[In0, Out]
  ): Expression[UniqueUnion[In, In0], Boolean]
}

object Expression {
  extension [In <: Tuple, A](self: Expression[In, A])
    // select multiple columns
    def &[In0 <: Tuple, B](
        that: Expression[In0, B]
    ): Expression[UniqueUnion[In, In0], MergeExpressionResults[A, B]] = ???
}

// Expression using a single field
type FieldExpression[N <: String & Singleton, A] =
  Expression[One[Field[N, A]], A]

// Make literal value an expression
def lit[A](value: A): Expression[EmptyTuple, A] = ???

// Typed DataFrame
trait DataFrame[A <: Tuple] {
  // Allows to select columns
  def select[F <: Tuple, Out](columns: Expression[F, Out]): DataFrame[
    AllColumnsExist[
      A,
      F,
      EmptyTuple, // Nothing selected - DataFrame with no fields
      SelectTuple[F, Out] // valid select
    ]
  ]

  // Allows to filter by existing columns condition
  def where[F <: Tuple](cond: Expression[F, Boolean]): DataFrame[
    AllColumnsExist[
      A,
      F,
      A, // Condition with no columns used - same fields
      A // valid field usage
    ]
  ]

  // Prepends column to DataFrame
  def withColumn[N <: String & Singleton, F <: Tuple, V](
      name: N,
      value: Expression[F, V]
  ): DataFrame[
    UniqueUnion[
      AllColumnsExist[
        A,
        F,
        F, // Needed for initial empty dataset
        A // When non empty, just return current schema, then it will append the new column
      ],
      One[Field[N, V]]
    ]
  ]

  // Allows to refer to a column of this DataFrame
  def apply[N <: String & Singleton](
      name: N
  ): FieldExpression[N, FindTypeOf[A, N]]
}

object DataFrame {
  // Starting point of the DSL
  val select: DataFrame[EmptyTuple] = ???
}
