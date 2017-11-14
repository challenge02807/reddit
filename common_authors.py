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

def dict_search (id, results, verbose):
    lst1 = []
    lst2 = []
    vals = []

    dict_outer = results[0]

    for k in dict_outer.keys():
        for dict_inner in results:
            for key in dict_inner.keys():
                if k != key:
                    inter_len = len(dict_inner[key].intersection(dict_outer[k]))
                    lst1.append(k)
                    lst2.append(key)
                    vals.append(inter_len)
        dict_outer[k] = set() # ensures only to search the dict key one time

    t_sort = time.time()

    if verbose: print("Process %i \t sorting!" % id)
    argsort = np.argsort(vals)
    lst1 = np.array(lst1)
    lst2 = np.array(lst2)
    vals = np.array(vals)

    lst1_sorted = lst1[argsort]
    lst2_sorted = lst2[argsort]
    vals_sorted = vals[argsort]

    if verbose: print("Process %i \t\t done! \t\t Time spend sorting: %.2f" % (id, time.time()-t_sort))
    return [lst1_sorted[-10:], lst2_sorted[-10:], vals_sorted[-10:]]

def main(verbose, part=-1, parallel=True):
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

    if verbose: print("Starting processes \t Time used: %.2f" % (time.time()-t0))
    with Pool(processes=cores) as pool:
        subreddits_authors = pool.starmap(searcher, args)

    if verbose: print("Searching in dicts \t Time used: %.2f" % (time.time()-t0))
    lst1 = []
    lst2 = []
    vals = []

    # if list is to big the memory can't handle multiprocessing
    if not parallel:
        s_a_copy = subreddits_authors.copy()
        for idx, dict_outer in enumerate(subreddits_authors):
            if verbose: print("Working on dict: %s \t\t Time used: %.2f" % (idx, time.time() - t0))
            for k in dict_outer.keys():
                for dict_inner in s_a_copy:
                    for key in dict_inner.keys():
                        if k != key:
                            inter_len = len(dict_inner[key].intersection(dict_outer[k]))
                            lst1.append(k)
                            lst2.append(key)
                            vals.append(inter_len)
                dict_outer[k] = set()  # ensures only to search the dict key one time
            del s_a_copy[0]  # ensures that we only search one time in dict list for each list item
    ### PARALLEL ###
    else:
        args = []
        for i in range(cores):
            inner = []
            inner.append(i)
            inner.append(subreddits_authors[i:])
            inner.append(verbose)
            args.append(inner)

        if verbose: print("Starting processes \t Time used: %.2f" % (time.time()-t0))
        with Pool(processes=cores) as pool2:
            output = pool2.starmap(dict_search, args)

        if verbose: print("Flatten lists \t Time used: %.2f" % (time.time()-t0))

        for v in output: lst1.extend(v[0])
        for v in output: lst2.extend(v[1])
        for v in output: vals.extend(v[2])

    if verbose: print('Find max values \t Time used: %.2f' % (time.time() - t0))
    # Finding 10 subreddit pairs with highest common authors
    # doing the same sort algoritm for each list to get matching pairs right with value respectively
    for i in range(10):
        max_index = np.argmax(vals)
        max_len = vals[max_index]
        vals[max_index] = -1
        red1 = lst1[max_index]
        red2 = lst2[max_index]
        red1_str = subreddits_dict[red1]
        red2_str = subreddits_dict[red2]
        print('%s common authors\t\tPair: (%s,%s) \t (%s,%s)' % (max_len, red1,red2,red1_str,red2_str))

    t1 = time.time()
    print("Query took %0.2f seconds" % (t1 - t0))

if __name__ == '__main__':
    main(verbose=True, part=10000, parallel=True)
