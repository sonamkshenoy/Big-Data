#!/usr/bin/python3

import sys

# Now finally, each line contains the : destination page, contribution to that destination page from some other page (which we have no clue of now)


currPage = None
totalContrib = 0

for line in sys.stdin:
    page, partialContrib = line.split()

    if(page == currPage):
        # Add up contributions
        totalContrib += float(partialContrib)

    else:

        # If not None
        if currPage:
            finalRank = 0.15 + 0.85 * totalContrib
            print("%s,%0.5f"%(currPage, finalRank))


        currPage = page
        finalRank = float(partialContrib)


# Remaining pages
if currPage:

    finalRank = 0.15 + 0.85 * totalContrib
    print("%s,%0.5f"%(currPage, finalRank))
