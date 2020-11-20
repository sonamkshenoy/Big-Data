#!/usr/bin/python3

import sys

# Store all initial page ranks before calculating contribution
ranks = dict()

rank_file = sys.argv([1]).strip()
f = open(rank_file, "r")

for line in f:
    page, rank = line.split(",")
    ranks[page.strip()] = float(rank)



# Calculate contribution of each page (pagerank of page / number of outgoing links from page)
# We have as input from stdin: page, outgoing links from the page
for line in sys.stdin:

    # -1 since first delimited value is the source page
    numLinks = len(line.split(",")) - 1

    # Source page
    srcPage = line.split(",")[0]

    # A page contributes 0 to itself
    print(srcPage, 0)

    # Calculate contribution
    pageContrib = ranks[srcPage]/numLinks

    # Now, in reducer, we want for a page, all "incoming" links to it and their page ranks
    # So print (page containing incoming link, its page rank)
    # Outgoing links present as: [1,2,3]

    outgoing_links = line.split(",")[1:].strip("[]")

    for destPage in outgoing_links:
        # Print the page to which current page is linking and how much this current page is contributing to that destination page
        # Print only if that page has a non-zero rank

        if destPage in ranks:
            print(destPage, pageContrib)
