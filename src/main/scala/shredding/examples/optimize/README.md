### Domain-Based Optimization Examples

This is a summary of some cases for the domain extraction at top level. 
Each case will have an input query, a materialization output for materialization_slender branch, 
and a materialization output for the master branch. The master branch has a trivial handling of 
some more cases (needs cleaned) and a let binding associating the free variables that were 
previously in the label to their counterpart in the subquery.

#### Case 1

All domain extraction at top level.

```
For c in C Union
  Sng((c_name := c.c_name, c_orders := For o in O Union
    If (o.o_custkey = c.c_custkey)
    Then Sng((o_orderdate := o.o_orderdate, o_parts := For l in L Union
      If (l.l_orderkey = o.o_orderkey)
      Then For p in P Union
        If (l.l_partkey = p.p_partkey)
        Then Sng((p_name := p.p_name, l_qty := l.l_quantity))))))
```

##### Materialization, no domains in materialization_slender

```
M_flat1 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Sng((c_name := c^F.c_name, c_orders := NewLabel(5, c^F, O^F, L^F, P^F)))

M_flat2 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((key := o^F.o_custkey, value := Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(3, o^F, L^F, P^F)))))

M_flat3 := For l^F in Lookup(lbl := L^F, dict := L^D) Union
  Sng((key := l^F.l_orderkey, value := For gb1 in For p^F in Lookup(lbl := P^F, dict := P^D) Union
    Sng((key := p^F.p_partkey, value := Sng((p_name := p^F.p_name, l_qty := l^F.l_quantity)))) Union
    If (l^F.l_partkey = gb1.key)
    Then gb1.value))
```

##### Materialization, no domains in master

```
M_flat1 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Sng((c_name := c^F.c_name, c_orders := NewLabel(5, c^F.c_custkey)))

M_flat2 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Let c^F.c_custkey = o^F.o_custkey In
  Sng((key := NewLabel(9, c^F.c_custkey), value := Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(8, o^F.o_orderkey)))))

M_flat3 := For l^F in Lookup(lbl := L^F, dict := L^D) Union
  Let o^F.o_orderkey = l^F.l_orderkey In
  Sng((key := NewLabel(10, o^F.o_orderkey), value := For gb1 in For p^F in Lookup(lbl := P^F, dict := P^D) Union
    Sng((key := p^F.p_partkey, value := Sng((p_name := p^F.p_name, l_qty := l^F.l_quantity)))) Union
    If (l^F.l_partkey = gb1.key)
    Then gb1.value))
```

#### Case 2

All domain extraction at top level, but one extraction is inside an And filter.

```
For c in C Union
  Sng((c_name := c.c_name, c_orders := For o in O Union
    If (o.o_custkey = c.c_custkey)
    Then Sng((o_orderdate := o.o_orderdate, o_parts := For l in L Union
      For p in P Union
        If (l.l_orderkey = o.o_orderkey AND l.l_partkey = p.p_partkey)
        Then Sng((p_name := p.p_name, l_qty := l.l_quantity))))))
```
##### Materialization, no domains in materialization_slender

```
M_flat1 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Sng((c_name := c^F.c_name, c_orders := NewLabel(5, c^F, O^F, L^F, P^F)))

M_flat2 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((key := o^F.o_custkey, value := Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(3, o^F, L^F, P^F)))))

M_flat3 := For l^F in Lookup(lbl := L^F, dict := L^D) Union
  For p^F in Lookup(lbl := P^F, dict := P^D) Union
    If (l^F.l_orderkey = o^F.o_orderkey AND l^F.l_partkey = p^F.p_partkey)
    Then Sng((p_name := p^F.p_name, l_qty := l^F.l_quantity))
```

##### Materialization, no domains in master

```
M_flat1 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Sng((c_name := c^F.c_name, c_orders := NewLabel(5, c^F.c_custkey)))

M_flat2 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Let c^F.c_custkey = o^F.o_custkey In
  Sng((key := NewLabel(9, c^F.c_custkey), value := Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(8, o^F.o_orderkey)))))

// this case is not handled, and the domain is not created
// so a dictionary is not returned
M_flat3 := For l^F in Lookup(lbl := L^F, dict := L^D) Union
  For p^F in Lookup(lbl := P^F, dict := P^D) Union
    If (l^F.l_orderkey = o^F.o_orderkey)
    Then If (l^F.l_partkey = p^F.p_partkey)
    Then Sng((p_name := p^F.p_name, l_qty := l^F.l_quantity))
```

#### Case 3

All domain extraction is at top level, but the label cannot be recreated
in the first level dictionary.

```
For o in O Union
  Sng((o_orderdate := o.o_orderdate, o_parts := For l in L Union
    For p in P Union
      If (l.l_orderkey = o.o_orderkey AND l.l_partkey = p.p_partkey)
      Then Sng((p_name := p.p_name, customers := For c in C Union
        If (c.c_custkey = o.o_custkey)
        Then Sng((c_name := c.c_name))))))
```
##### Materialization, no domains in materialization_slender

```
M_flat1 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(5, o^F, L^F, P^F, C^F)))

M_flat2 := For l^F in Lookup(lbl := L^F, dict := L^D) Union
  For p^F in Lookup(lbl := P^F, dict := P^D) Union
    If (l^F.l_orderkey = o^F.o_orderkey AND l^F.l_partkey = p^F.p_partkey)
    Then Sng((p_name := p^F.p_name, customers := NewLabel(2, C^F, o^F)))

M_flat3 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Sng((key := c^F.c_custkey, value := Sng((c_name := c^F.c_name))))
```

##### Materialization, no domains in master

```
M_flat1 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(5, o^F.o_orderkey, o^F.o_custkey)))

// domain is lost here, so o^F is free
M_flat2 := For l^F in Lookup(lbl := L^F, dict := L^D) Union
  For p^F in Lookup(lbl := P^F, dict := P^D) Union
    If (l^F.l_orderkey = o^F.o_orderkey)
    Then If (l^F.l_partkey = p^F.p_partkey)
    Then Sng((p_name := p^F.p_name, customers := NewLabel(9, o^F.o_custkey)))

// this expression is ok
M_flat3 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Let o^F.o_custkey = c^F.c_custkey In
  Sng((key := NewLabel(10, o^F.o_custkey), value := Sng((c_name := c^F.c_name))))
```

#### Case 4 

Domain extraction is not at top level and the label
cannot be recreated in the first level dictionary.

```
For o in O Union
  Sng((o_orderdate := o.o_orderdate, o_parts := For p in P Union
    For l in L Union
      If (l.l_orderkey = o.o_orderkey AND l.l_partkey = p.p_partkey)
      Then Sng((p_name := p.p_name, customers := For c in C Union
        If (c.c_custkey = o.o_custkey)
        Then Sng((c_name := c.c_name))))))
```

##### Materialization, no domains in materialization_slender

```
M_flat1 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(5, o^F, L^F, P^F, C^F)))

M_flat2 := For p^F in Lookup(lbl := P^F, dict := P^D) Union
  For l^F in Lookup(lbl := L^F, dict := L^D) Union
    If (l^F.l_orderkey = o^F.o_orderkey AND l^F.l_partkey = p^F.p_partkey)
    Then Sng((p_name := p^F.p_name, customers := NewLabel(2, C^F, o^F)))

M_flat3 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Sng((key := c^F.c_custkey, value := Sng((c_name := c^F.c_name))))
```

##### Materialization, no domains in master

```
M_flat1 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((o_orderdate := o^F.o_orderdate, o_parts := NewLabel(5, o^F.o_orderkey, o^F.o_custkey)))

// this captures o^F.o_orderkey binding, so not at top level is covered
// but the label still cannot be recreated
M_flat2 := For p^F in Lookup(lbl := P^F, dict := P^D) Union
  For l^F in Lookup(lbl := L^F, dict := L^D) Union
    Let o^F.o_orderkey = l^F.l_orderkey In
    Sng((key := NewLabel(10, o^F.o_orderkey), value := If (l^F.l_partkey = p^F.p_partkey)
    Then Sng((p_name := p^F.p_name, customers := NewLabel(9, o^F.o_custkey)))))

// this case is ok
M_flat3 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Let o^F.o_custkey = c^F.c_custkey In
  Sng((key := NewLabel(11, o^F.o_custkey), value := Sng((c_name := c^F.c_name))))
```

#### Case 5

Domain extraction is across multiple relations, and one of the
extractions is not at top level.

```
For o in O Union
  Sng((o_orderdate := o.o_orderdate, custparts := For c in C Union
    If (c.c_custkey = o.o_custkey)
    Then For p in P Union
      For l in L Union
        If (l.l_orderkey = o.o_orderkey AND l.l_partkey = p.p_partkey)
        Then Sng((c_name := c.c_name, p_name := p.p_name))))
```
##### Materialization, no domains in materialization_slender

```
M_flat1 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((o_orderdate := o^F.o_orderdate, custparts := NewLabel(4, o^F, L^F, P^F, C^F)))

M_flat2 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Sng((key := c^F.c_custkey, value := For p^F in Lookup(lbl := P^F, dict := P^D) Union
    For l^F in Lookup(lbl := L^F, dict := L^D) Union
      If (l^F.l_orderkey = o^F.o_orderkey AND l^F.l_partkey = p^F.p_partkey)
      Then Sng((c_name := c^F.c_name, p_name := p^F.p_name))))
```

##### Materialization, no domains in master

```
M_flat1 := For o^F in Lookup(lbl := O^F, dict := O^D) Union
  Sng((o_orderdate := o^F.o_orderdate, custparts := NewLabel(4, o^F.o_custkey, o^F.o_orderkey)))

// cases are handled over multiple relations
M_flat2 := For c^F in Lookup(lbl := C^F, dict := C^D) Union
  Let o^F.o_custkey = c^F.c_custkey In
  Sng((key := NewLabel(8, o^F.o_custkey), value := For p^F in Lookup(lbl := P^F, dict := P^D) Union
    For l^F in Lookup(lbl := L^F, dict := L^D) Union
      Let o^F.o_orderkey = l^F.l_orderkey In
      Sng((key := NewLabel(7, o^F.o_orderkey), value := If (l^F.l_partkey = p^F.p_partkey)
      Then Sng((c_name := c^F.c_name, p_name := p^F.p_name))))))
```

#### Case 6

Domain extraction is done inside a lookup.

```
For v in variants Union
  Sng((contig := v.contig, start := v.start, cases := (For g in v.genotypes Union
    For c in clinical Union
      If (g.sample = c.sample)
      Then Sng((case := c.iscase, genotype := g.call))).groupBy+((case := x1.case)), x1.genotype)))
```
##### Materialization, no domains in materialization_slender

This case hits an error because [linearizeNoDomains isn't handling the BagDictLet case](https://github.com/jacmarjorie/shredder/blob/materialization_slender/src/main/scala/shredding/nrc/Linearization.scala#L190). There is a [fix for 
this in master](https://github.com/jacmarjorie/shredder/blob/master/src/main/scala/shredding/nrc/Linearization.scala#L31).

##### Materialization, no domains in master

```
M_flat1 := For v^F in Lookup(lbl := variants^F, dict := variants^D) Union
  Sng((contig := v^F.contig, start := v^F.start, cases := NewLabel(3, v^F.genotypes)))

// v^F and v^D are free, but v^D.genotypes is a reference to an input dictionary
// we want to iterate over all the values in this case
M_flat2 := (For g^F in Lookup(lbl := v^F.genotypes, dict := v^D.genotypes) Union
  For c^F in Lookup(lbl := clinical^F, dict := clinical^D) Union
    If (g^F.sample = c^F.sample)
    Then Sng((case := c^F.iscase, genotype := g^F.call))).groupBy+((case := x1.case)), x1.genotype)
```
