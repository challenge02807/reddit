import sqlite3
import time,random
import numpy as np
from multiprocessing import Pool
from multiprocessing import cpu_count

symbols = '\n`~!@#$%^&*()_-+={[]}|\\:;"\'<>.?/,'
trantab = str.maketrans(symbols, " "*len(symbols))
deletions = ''.join(ch for ch in map(chr,range(256)) if ch not in symbols)

def getWords(comment):
    words = set()
    for w in (comment.lower().translate(str.maketrans(symbols, " "*len(symbols)))).split(" "):
        if w:
            words.add(w)
    return words

def searcher(id,subreddits,num_reds):
    max_reds = [("",-1)]*10
    con = sqlite3.connect('reddit.db')
    c = con.cursor()
    for i,red in enumerate(subreddits):
        vocab = set()
        for row in c.execute('SELECT body FROM comments INNER JOIN subreddits ON comments.subreddit_id = subreddits.id WHERE subreddits.id = "'+red+'"'):
            vocab.update(getWords(row[0]))
        vocab_len = len(vocab)
        for i,m in enumerate(max_reds):
            if vocab_len > m[1]:
                max_reds[i] = (red,vocab_len)
                break

    print("Process %i done!" % id)
    return max_reds

t0 = time.time()

con = sqlite3.connect('reddit.db')
c = con.cursor()

subreddits_dict = {}
for row in c.execute('SELECT id,name FROM subreddits'):
    subreddits_dict[row[0]] = row[1]

subreddits = list(subreddits_dict.keys())

subreddits = random.sample(subreddits, len(subreddits))

cores = cpu_count() * 4
num_reds = int(float(len(subreddits)/cores))

parts = []
for i in range(cores):
    if i != cores-1:
        parts.append(subreddits[num_reds*i:num_reds*(i+1)])
        #print("Process %i --> [%i:%i]" % (i,num_reds * i, num_reds * (i + 1)))
    else:
        parts.append(subreddits[num_reds * i:])
        #print("Process %i --> [%i:%s]" % (i,num_reds * i, ""))

args = []
for i in range(cores):
    inner = []
    inner.append(i)
    inner.append(parts[i])
    inner.append(num_reds)
    args.append(inner)

with Pool(processes=cores) as pool:
    results = pool.starmap(searcher, args)
    lengths = []
    reds = []
    for res in results:
        for elem in res:
            lengths.append(elem[1])
            reds.append(elem[0])

    for i in range(10):
        max_index = np.argmax(lengths)
        max_len = lengths[max_index]
        lengths[max_index] = -1
        red = subreddits_dict[reds[max_index]]
        print("Subreddit %s has vocabulary of %i words" % (red,max_len))

t1 = time.time()
print("Query took %0.2f seconds" % (t1-t0))
