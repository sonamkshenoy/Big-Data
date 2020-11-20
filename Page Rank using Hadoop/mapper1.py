#!/usr/bin/python3

import sys

currPage = None
links = []


# Stream lines from stdin (like readlines() for a physical file) and print all links from the page as soon as all links belonging to that page get over
# Through the file "iterate-hadoop.sh" the text file containing links is passed through stdin

for line in sys.stdin:
    # Input is of format: from_page to_page
    page, link = line.split()

    # New page links
    if(page != currPage and currPage):

        # When page exists (not None)
        if(currPage);
            print(currPage, ", [",  ",".join(links), "]")

        currPage = page
        links = [link]

    # Link belonging to current page
    else:
        links.append(link)


# For the last page
for link in pageLinks:
    print(link, ", [",  ",".join(pageLinks[link], "]")

