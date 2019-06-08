### TPC-H Queries

Derived from Slender Sigmod paper

#### Query 1:

```
For c in C Union
  Sng((c_name := c.c_name, c_orders := For o in O Union
    If (o.o_custkey = c.c_custkey)
    Then Sng((o_orderdate := o.o_orderdate, o_parts := For l in L Union
      For p in P Union
        If (l.l_orderkey = o.o_orderkey AND l.l_partkey = p.p_partkey)
        Then Sng((p_name := p.p_name, l_qty := l.l_quantity))))))
```

#### Query 2:

```
For s in S Union
  Sng((s_name := s.s_name, customers2 := For l in L Union
    If (s.s_suppkey = l.l_suppkey)
    Then For o in O Union
      If (o.o_orderkey = l.l_orderkey)
      Then For c in C Union
        If (c.c_custkey = o.o_custkey)
        Then Sng((c_name2 := c.c_name))))
```

#### Query 3: 

```
For p in P Union
  Sng((p_name := p.p_name, suppliers := For ps in PS Union
    If (ps.ps_partkey = p.p_partkey)
    Then For s in S Union
      If (s.s_suppkey = ps.ps_suppkey)
      Then Sng((s_name := s.s_name)), customers := For l in L Union
    If (l.l_partkey = p.p_partkey)
    Then For o in O Union
      If (o.o_orderkey = l.l_orderkey)
      Then For c in C Union
        If (c.c_custkey = o.o_custkey)
        Then Sng((c_name := c.c_name))))
  ```
