# -*- coding: utf-8 -*-
from __future__ import division,print_function
import sqlite3
import time
import heapq
from multiprocessing import Pool
from multiprocessing import cpu_count

cores = cpu_count() * 3

con = sqlite3.connect('reddit.db')
c = con.cursor()

def searcher(subreddits):
    
    depth_sum = 0
    comm_sum = 0
    # finds all top level comments by t3
    c.execute("""SELECT id FROM comments WHERE subreddit_id = ?	AND parent_id LIKE 't3%' """, subreddits)
    
    #find the comment depth using recursive function in sql
    for com in c.fetchall():
		
        c.execute(""" WITH deep (id,depth) AS 
					(
						values (?,0)
		
						UNION ALL
		
						SELECT comments.id, deep.depth+1
						FROM comments JOIN deep on comments.parent_id = deep.id
					)
					Select max(depth) from deep
               """, [com[0]])

        #each thread and toplevel comments
        query = c.fetchall()

        for i in query:
            depth_sum += i[0]

        comm_sum += len(query) 
    # 0 if no comments
    if comm_sum == 0:
        return ( 0, subreddits)
    else:
        return (depth_sum/comm_sum, subreddits)


if __name__ == '__main__':

    t1 = time.time()

    # sql to get subreddits ids.
    c.execute("SELECT id FROM subreddits")
    
    #run multiprossing
    with Pool(processes=cores) as pool:
        results = pool.map(searcher, c.fetchall())
        pool.close()
        #Find deepest - number of deepest set in the nlargest arg1. 
        deepest = heapq.nlargest(10, results)

        #results
        result_for_file = []
        fetch = ""
        for i in deepest:
            c.execute("SELECT name FROM subreddits  WHERE id = ?",i[1])
            fetch = c.fetchall()[0][0]
            print (i, fetch)
            result_for_file.append(str(i[0]) +"     " + str(i[1][0]) + "      " + fetch)

    t2 = time.time()

    print ("Query took: {}".format(t2-t1))
