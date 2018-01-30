# MapReduce
MapReduce based analytical operations over large text data within the Hadoop framework.
Input and Data: a large text data set, which contains the complete edit history (allrevisions, all pages) of all Wikipedia since its inception till January 2008.”

Your code will need the first line of each record, which consists of the following attributes:

article_id: an integer, uniquely identifying each page.

rev_id: an integer, uniquely identifying each revision.

article_title: the title of the page.

timestamp: the exact time of the revision, in the format of ISO8601. E.g., 12:34:00 UTC 28 September 1990 becomes 1990-09-28T12:34:00Z, where T separates the date from the time part, and Z denotes the time is in UTC.

[ip:]username: the name of the reviewer who performs the revision.

user_id: an integer uniquely identifying each user who performs the revision.

Part 1: Finding the most active users.
Given a time interval (earlier timestamp0, later timestamp1), and a number N, print the top N users with the most number of revisions. The output should be sorted first by the number of revisions and then by user_id.
Where sorting is required, the output should be in descending numeric order by revision number first, then in ascending numeric order by user_id.

Part 2: Finding the articles with the highest number of revisions.
Similar to Part 1, given a time interval (earlier timestamp, later timestamp), and a number N, print the top N articles with the most number of revisions.
