#!/usr/bin/python3

import sys

rank_file = sys.argv[1].strip()

# Open in append mode
f = open(rank_file, "w+")

# Stdin now contains source page, and array of links it points to
for line in sys.stdin:
    src_page = line.split(",")[0]

    # Write page name to rank file with initial rank = 1
    f.write("%s,%0.5f\n"%(src_page, 1))

    # Pass output from mapper1 to next mapper (the output won't remain, have to pass manually)
    print(line, end='')

f.close()

