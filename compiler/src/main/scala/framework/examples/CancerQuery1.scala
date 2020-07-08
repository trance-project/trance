package framework.examples

import framework.common._

object exp extends CancerSchema {
  val name = "occurrence_mapped"
  val query =
    ForeachUnion(or, occurrences,
      ForeachUnion(br, biospec,
        IfThenElse(Cmp(OpEq, or("donorId"), br("bcr_patient_uuid")),
          Singleton(Tuple(
            "temp" -> Const(0.0, DoubleType)
          ))
        )

      )
    )
  val program = Program(Assignment(name, query))
}