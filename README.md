# BUMP STORAGE ENGINE

// In progress ...

```
            BBBBBBBB   UU    UU  MM          MM  PPPPPPP   DDDDDDD   BBBBBBB
            B      BB  UU    UU  MMM        MMM  P    PPP  D    DDD  B      BB
            BBBBBBBB   UU    UU  MMMM      MMMM  PPPPPPP   D    DDD  BBBBBBB
            B      BB  UU    UU  MM MM    MM MM  P         D    DDD  B      BB
            BBBBBBBB    UUUUU    MM  MM  MM  MM  P         DDDDDDD   BBBBBBB
```

## Plans to increase read speed
* Introduce Block Cache
* Introduce Snappy Compression Algorithm to reduce offset scanned to retrieve blocks on the disk
* Introduce LCS 

This will signnificantly improve the read speed for now  we only have key index, bloomfilters and sized tier (grouped into buckets)