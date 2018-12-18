package shredding.nrc

/**
  * Type checking constructs
  */

trait EmbedNRC extends NRCExprs {

  /**
    * Automatically lift basic types into their DSL representation
    */
  implicit def liftString(x: String): Expr[String] = Const(x)

  implicit def liftInt(x: Int): Expr[Int] = Const(x)

  implicit def liftDouble(x: Double): Expr[Double] = Const(x)

  implicit def liftBoolean(x: Boolean): Expr[Boolean] = Const(x)

  implicit def liftSymbol[A](x: Symbol): Sym[A] = Sym[A](x)

  /**
    * DSL constructors
    */
  implicit def bagOps[A](self: Expr[TBag[A]]) = new {
    def ForeachUnion[B](f: Expr[A] => Expr[TBag[B]]): Expr[TBag[B]] = {
      val x = Sym[A]('x)
      new ForeachUnion(x, self, f(x))
    }

    def ForeachYield[B](f: Expr[A] => Expr[B]): Expr[TBag[B]] =
      ForeachUnion((x: Expr[A]) => Singleton(f(x)))

    def Union(rhs: Expr[TBag[A]]): Expr[TBag[A]] =
      new Union(self, rhs)

    def ForeachMapunion[B](f: Expr[A] => Expr[TMap[Label[B], TBag[B]]]): Expr[TMap[Label[B], TBag[B]]] = {
      val x = Sym[A]('x)
      new ForeachMapunion(x, self, f(x))
    }

  }

  implicit def mapOps[A](self: Expr[TMap[Label[A], TBag[A]]]) = new { 
    def Mapunion(rhs: Expr[TMap[Label[A], TBag[A]]]): Expr[TMap[Label[A], TBag[A]]] = 
      new Mapunion(self, rhs)

    def ForeachDomain[B](f: Expr[A] => Expr[TBag[B]]): Expr[TBag[B]] = {
      val w = Sym[A]('w)
      new ForeachDomain(w, self, f(w))
    }
  }

  implicit def flattenOp[A](self: Expr[TBag[TBag[A]]]) = new {
    def Flatten: Expr[TBag[A]] = self.ForeachUnion(e => e)
  }

  implicit def projectOps1[A](self: Expr[TTuple1[A]]) = new {
    def Project1: Expr[A] = Project(self, 1)
    def _1: Expr[A] = Project(self, 1)
  }

  implicit def projectOps2[A, B](self: Expr[TTuple2[A, B]]) = new {
    def Project1: Expr[A] = Project(self, 1)
    def Project2: Expr[B] = Project(self, 2)
    def _1: Expr[A] = Project(self, 1)
    def _2: Expr[B] = Project(self, 2)
  }

  implicit def projectOps3[A, B, C](self: Expr[TTuple3[A, B, C]]) = new {
    def Project1: Expr[A] = Project(self, 1)
    def Project2: Expr[B] = Project(self, 2)
    def Project3: Expr[C] = Project(self, 3)
    def _1: Expr[A] = Project(self, 1)
    def _2: Expr[B] = Project(self, 2)
    def _3: Expr[C] = Project(self, 3)
  }

  implicit def eqOp[A,B](self: Expr[A]) = new {
    def Equals(e: Expr[B]): Expr[Boolean] = Eq(self, e) 
  }
}

