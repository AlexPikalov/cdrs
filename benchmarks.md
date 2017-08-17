## CDRS performance benchmarks

These benchmarks contain two type of measurements:

- performance during parsing a frame that contains one row of values of a
certain type
- performance during type conversion when you try to convert a row into
rust types/structures

#### Native types

```
test blob_body_parse    ... bench:       1,595 ns/iter (+/- 238)
test blob_convert       ... bench:         212 ns/iter (+/- 115)
test counter_body_parse ... bench:       2,124 ns/iter (+/- 1,160)
test counter_convert    ... bench:         396 ns/iter (+/- 257)
test float_body_parse   ... bench:       1,908 ns/iter (+/- 310)
test float_convert      ... bench:         381 ns/iter (+/- 167)
test inet_body_parse    ... bench:       2,040 ns/iter (+/- 293)
test inet_convert       ... bench:         387 ns/iter (+/- 85)
test integer_body_parse ... bench:       2,473 ns/iter (+/- 419)
test integer_convert    ... bench:         549 ns/iter (+/- 129)
test string_body_parse  ... bench:       2,475 ns/iter (+/- 588)
test string_convert     ... bench:         674 ns/iter (+/- 132)
test time_body_parse    ... bench:       1,505 ns/iter (+/- 222)
test time_convert       ... bench:         163 ns/iter (+/- 39)
test uuid_body_parse    ... bench:       1,464 ns/iter (+/- 227)
test uuid_convert       ... bench:         168 ns/iter (+/- 32)
```

#### Collection types (List, Set, Map)

```
test list_body_parse             ... bench:       2,694 ns/iter (+/- 444)
test list_convert                ... bench:       4,953 ns/iter (+/- 720)
test list_v4_body_parse          ... bench:       2,578 ns/iter (+/- 933)
test list_v4_convert             ... bench:       4,929 ns/iter (+/- 658)
test map_body_parse              ... bench:       3,174 ns/iter (+/- 1,222)
test map_convert                 ... bench:       8,087 ns/iter (+/- 1,540)
test map_without_blob_body_parse ... bench:       3,787 ns/iter (+/- 772)
test map_without_blob_convert    ... bench:       7,867 ns/iter (+/- 1,022)
```
