### Simple Example Queries

#### Flat

These are queries that operate on flat inputs only.

##### q1: Standard for with a projection
```
For x in R Union
  Sng((o1 := x.a))
```
#### q2: Total multiplicity of a bag at top level
```
TotalMult(For x in R Union Sng(x))
```

#### q3: Filter condition
```
For x in R Union
  if (x.a > 40) 
  then Sng((o1 := x.b))
```

#### Nested

These are queries on nested data that either query at top level, or query deeper into the structure.

##### q1: Michael's first grouping example
```
For x in R Union
  Sng((o5 := x.h, o6 := For w in x.j Union
    Sng((o7 := w.m, o8 := Total(w.k)))))
```

#### q2: similar to above, but multiplicity at top level
```
For x in R Union
  Sng((o5 := x.h, o6 := Total(x.j)))
```

#### q3: filter at top level
```
For x in R Union
  If (x.h > 60)
  Then Sng((w := x.j))
```

#### q4: filter at top level with a multiplicity 
```
For x in R Union
  If (x.h > 60)
  Then Sng((w := Total(x.j)))
```

#### q5: self join, with a join condition
```
For x in R Union
  For x2 in R Union
    If (x.h = x2.h)
    Then Sng((x := x.j, x2 := x2.j))
```

#### q6: filter at top level, with a multiplicity (similar to q4, but with different data)
```
For x in R Union
  If (x.a > 40)
  Then Sng((o1 := x.a, o2 := Total(x.b)))
```

#### q7: self cross product, loop over nested bag and take multiplicity
```
For x in R Union
  For x1 in R Union
    For x2 in x.b Union
      Sng((o1 := x.a, o2 := Total(x.b)))
```

#### q8: michael's filter an inner bag example
```
For x in R Union
  Sng((a' := x.a, s1' := For x2 in x.b Union
    If (2 > x2.c)
    Then Sng(x2)))
```

#### q9: similar to q8, but let's share a dictionary 
```
For x in R Union
  Sng((a' := x.a, s1' := For x1 in x.s1 Union
    If (5 > x1.c)
    Then Sng(x1), s2' := For x1 in x.s2 Union
    If (x1.c > 6)
    Then Sng(x1)))
```
