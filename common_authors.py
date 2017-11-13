import sqlite3
import time,random
import numpy as np
from multiprocessing import Pool
from multiprocessing import cpu_count

def searcher(id,subreddits, verbose):
    con = sqlite3.connect('reddit.db')
    c = con.cursor()
    authors_dict = {}
    for i,red in enumerate(subreddits):
        authors = set()
        for row in c.execute('SELECT author_id FROM comments WHERE subreddit_id = "'+red+'"'):
            authors.add(row[0])

        authors_dict[red] = authors

    if verbose: print("Process %i \t\t done!" % id)
    return authors_dict

def main(verbose, part=-1):
    t0 = time.time()

    con = sqlite3.connect('reddit.db')
    c = con.cursor()

    subreddits_dict = {}
    for row in c.execute('SELECT id,name FROM subreddits'):
        subreddits_dict[row[0]] = row[1]

    subreddits = list(subreddits_dict.keys())
    len_sub = len(subreddits)
    if part > 0: subreddits = subreddits[-part:]
    if verbose: print('No. of subreddits: %s out of %s' % (len(subreddits),len_sub))
    subreddits = random.sample(subreddits, len(subreddits))

    cores = cpu_count() * 4
    num_reds = int(float(len(subreddits)/cores))

    parts = []
    for i in range(cores):
        if i != cores-1:
            parts.append(subreddits[num_reds*i:num_reds*(i+1)])
        else:
            parts.append(subreddits[num_reds * i:])

    args = []
    for i in range(cores):
        inner = []
        inner.append(i)
        inner.append(parts[i])
        inner.append(verbose)
        args.append(inner)

    results = []
    if verbose: print("Number of processes: %s, Results length: %s" % (cores, len(results)))
    if verbose: print("Starting processes \t Time used: %.2f" % (time.time()-t0))
    with Pool(processes=cores) as pool:
        results = pool.starmap(searcher, args)

    if verbose: print("Fetching result \t Time used: %.2f" % (time.time()-t0))
    lst1 = []
    lst2 = []
    vals = []

    results2 = results.copy()
    for idx, dict_outer in enumerate(results):
        print("Working on dict: %s \t\t Time used: %.2f" % (idx, time.time() - t0))
        for k in dict_outer.keys():
            for dict_inner in results2:
                for key in dict_inner.keys():
                    if k != key:
                        inter_len = len(dict_inner[key].intersection(dict_outer[k]))
                        lst1.append(k)
                        lst2.append(key)
                        vals.append(inter_len)
            dict_outer[k] = set()
        del results2[0]

    if verbose: print('Find max values \t Time used: %.2f' % (time.time() - t0))

    for i in range(10):
        max_index = np.argmax(vals)
        max_len = vals[max_index]
        vals[max_index] = -1
        red1 = lst1[max_index]
        red2 = lst2[max_index]
        red1_str = subreddits_dict[red1]
        red2_str = subreddits_dict[red2]
        print('Pair: (%s,%s) \t (%s,%s) No. of common authors: %s' % (red1,red2,red1_str,red2_str,max_len))

    t1 = time.time()
    print("Query took %0.2f seconds" % (t1 - t0))

if __name__ == '__main__':
    main(verbose=True)
