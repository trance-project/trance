## Shredder

To compile and run:
```
sbt run
```

Organization of `src/main/scala/shredding`:
* `common`: types, operators, and variable definitions that are shared across the pipeline
* `nrc`: soure nrc, target nrc, nrc extensions for shredding (dictionaries, labels), shredding, and linearization  
* `runtime`: runtime specifics for nrc and shredded nrc 
* `wmcc`: weighted monad comprehension calculus (wmcc), which algebra operators (select, reduce, join, etc.).
* `generator`: code generators, currently just native scala
* `loader`: functionality to load data for executing queries, currently csv readers used for reading tpch from a file
* `examples`: a directory of example queries 
* `utils`: utility methods used throughout




List(Rec(cname:Test Customer4,customers:
List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))),
Rec(cname:Test Customer3,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer2,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer1,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer5,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer6,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))))


List(Rec(cname:Test Customer4,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer3,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer2,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer1,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer5,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))), Rec(cname:Test Customer6,customers:List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B), Rec(s_name:supplier B), Rec(s_name:supplier C), Rec(s_name:supplier C))))

Normal Q6:
List(
    Rec(cname:Test Customer1,customers:List(List(List(Rec(s_name:supplier A)), List(Rec(s_name:supplier A)), List(Rec(s_name:supplier A))), List(List(Rec(s_name:supplier B))), List(), List())),
    Rec(cname:Test Customer2,customers:List(List(), List(List(Rec(s_name:supplier B))), List(List(Rec(s_name:supplier C))), List())),
    Rec(cname:Test Customer3,customers:List(List(), List(), List(List(Rec(s_name:supplier C))), List())),
    Rec(cname:Test Customer4,customers:List(List(), List(), List(), List())),
    Rec(cname:Test Customer5,customers:List(List(), List(), List(), List())),
    Rec(cname:Test Customer6,customers:List(List(), List(), List(), List())))
Shredded Q6:
List(
    List(Rec(lbl:Rec(C__F:1,Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1)))),
    List(Rec(k:Rec(C__F:1,Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1)),v:
                            List(   Rec(cname:Test Customer1,customers:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:
                                        Rec(c_custkey:1,c_name:Test Customer1,c_nationkey:1))),
                                    Rec(cname:Test Customer2,customers:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:2,c_name:Test Customer2,c_nationkey:1))),
                                    Rec(cname:Test Customer3,customers:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:3,c_name:Test Customer3,c_nationkey:1))),
                                    Rec(cname:Test Customer4,customers:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:4,c_name:Test Customer4,c_nationkey:2))),
                                    Rec(cname:Test Customer5,customers:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:5,c_name:Test Customer5,c_nationkey:3))),
                                    Rec(cname:Test Customer6,customers:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:6,c_name:Test Customer6,c_nationkey:3)))))),
    List(   Rec(lbl:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:1,c_name:Test Customer1,c_nationkey:1))),
            Rec(lbl:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:2,c_name:Test Customer2,c_nationkey:1))),
            Rec(lbl:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:3,c_name:Test Customer3,c_nationkey:1))),
            Rec(lbl:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:4,c_name:Test Customer4,c_nationkey:2))),
            Rec(lbl:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:5,c_name:Test Customer5,c_nationkey:3))),
            Rec(lbl:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:6,c_name:Test Customer6,c_nationkey:3)))),
    List(Rec(k:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:1,c_name:Test Customer1,c_nationkey:1)),v:
                            List(Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier A), Rec(s_name:supplier B))),
         Rec(k:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:2,c_name:Test Customer2,c_nationkey:1)),v:List(Rec(s_name:supplier B), Rec(s_name:supplier C))),
         Rec(k:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:3,c_name:Test Customer3,c_nationkey:1)),v:List(Rec(s_name:supplier C))),
         Rec(k:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:4,c_name:Test Customer4,c_nationkey:2)),v:List()),
         Rec(k:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:5,c_name:Test Customer5,c_nationkey:3)),v:List()),
         Rec(k:Rec(Q2__F:Rec(S__F:1,C__F:1,L__F:1,O__F:1),c__F:Rec(c_custkey:6,c_name:Test Customer6,c_nationkey:3)),v:List())))



M_ctx1 := Sng((lbl := NewLabel(11, S^F, O^F, L^F, C^F)))
M_flat1 := For l1 in M_ctx1 Union
  Extract l1.lbl as (L^F, C^F, O^F, S^F) In
  Sng((k := l1.lbl, v := For c^F in Lookup(lbl := C^F, dict := C^D) Union
    Sng((cname := c^F.c_name, customers := NewLabel(9, S^F, c^F, O^F, L^F, C^F)))))"
M_ctx2 := DeDup(For kv1 in M_flat1 Union
  For xF1 in kv1.v Union
    Sng((lbl := xF1.customers)))
M_flat2 := For l2 in M_ctx2 Union
  Extract l2.lbl as (L^F, O^F, C^F, c^F, S^F) In
  Sng((k := l2.lbl, v := Let x^D = (s_name := Nil, customers2 := (NewLabel(4, O^F, L^F, s^F, C^F) ->
    flat :=
      For l^F in Lookup(lbl := L^F, dict := L^D) Union
        If (s^F.s_suppkey = l^F.l_suppkey)
        Then For o^F in Lookup(lbl := O^F, dict := O^D) Union
          If (o^F.o_orderkey = l^F.l_orderkey)
          Then For c^F in Lookup(lbl := C^F, dict := C^D) Union
            If (c^F.c_custkey = o^F.o_custkey)
            Then Sng((c_name2 := c^F.c_name)),
    tupleDict :=
      (c_name2 := Nil)
  )) In
  For x^F in For s^F in Lookup(lbl := S^F, dict := S^D) Union
    Sng((s_name := s^F.s_name, customers2 := NewLabel(4, O^F, L^F, s^F, C^F))) Union
    For cz^F in Lookup(lbl := x^F.customers2, dict := x^D.customers2) Union
      If (c^F.c_name = cz^F.c_name2)
      Then Sng((s_name := x^F.s_name))))"
Shredded q6: M_ctx1 := { (lbl := (S__F := S__F,C__F := C__F,L__F := L__F,O__F := O__F)) }

M_flat1 := { { (k := x11.lbl,v := { { { (cname := x13.c_name,customers := (O__F := x11.lbl.O__F,C__F := x11.lbl.C__F,S__F := x11.lbl.S__F,c__F := x13,L__F := x11.lbl.L__F)) } | x13 <- x12._2 } | x12 <- C__D._1, x11.lbl.C__F == x12._1 }) } | x11 <- M_ctx1 }

M_ctx2 := DeDup({ { { (lbl := x15.customers) } | x15 <- x14.v } | x14 <- M_flat1 })

M_flat2 := { { (k := x16.lbl,v := { { { { { { { { { if (x16.lbl.c__F.c_name == x22.c_name) then { { (s_name := x24.s_name) } } | x22 <- x21._2, x22.c_custkey == x20.o_custkey } | x21 <- C__D._1, x16.lbl.C__F == x21._1 } | x20 <- x19._2, x20.o_orderkey == x18.l_orderkey } | x19 <- O__D._1, x16.lbl.O__F == x19._1 } | x18 <- x17._2, s__F.s_suppkey == x18.l_suppkey } | x17 <- L__D._1, x16.lbl.L__F == x17._1 } } | x24 <- x23._2 } | x23 <- S__D._1, x16.lbl.S__F == x23._1 }) } | x16 <- M_ctx2 }
M_ctx1 := List(Rec(lbl:Rec(S__F:1,C__F:1,L__F:1,O__F:1)))

M_flat1 := List(Rec(k:Rec(S__F:1,C__F:1,L__F:1,O__F:1),v:List(Rec(cname:Test Customer1,customers:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:1,c_name:Test Customer1,c_nationkey:1),L__F:1)), Rec(cname:Test Customer2,customers:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:2,c_name:Test Customer2,c_nationkey:1),L__F:1)), Rec(cname:Test Customer3,customers:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:3,c_name:Test Customer3,c_nationkey:1),L__F:1)), Rec(cname:Test Customer4,customers:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:4,c_name:Test Customer4,c_nationkey:2),L__F:1)), Rec(cname:Test Customer5,customers:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:5,c_name:Test Customer5,c_nationkey:3),L__F:1)), Rec(cname:Test Customer6,customers:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:6,c_name:Test Customer6,c_nationkey:3),L__F:1)))))

M_ctx2 := List(Rec(lbl:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:1,c_name:Test Customer1,c_nationkey:1),L__F:1)), Rec(lbl:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:2,c_name:Test Customer2,c_nationkey:1),L__F:1)), Rec(lbl:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:3,c_name:Test Customer3,c_nationkey:1),L__F:1)), Rec(lbl:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:4,c_name:Test Customer4,c_nationkey:2),L__F:1)), Rec(lbl:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:5,c_name:Test Customer5,c_nationkey:3),L__F:1)), Rec(lbl:Rec(O__F:1,C__F:1,S__F:1,c__F:Rec(c_custkey:6,c_name:Test Customer6,c_nationkey:3),L__F:1)))




 REDUCE[ (cname := x13.c_name,customers := x16) / true ]( <-- (x13,x16) --
    NEST[ U / (s_name := x14.s_name) / (x13), true / (x14,x15) ]
                (  <-- (x13,x14,x15) -- OUTERUNNEST[ x14.customers2 / x13.c_name == x15.c_name2 ]
                ( <-- (x13,x14) -- (C) OUTERJOIN[true = true](Q2))))