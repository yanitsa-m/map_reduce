
# coding: utf-8

# ## MapReduce in Python
# ### Exercise 0: Word Count

# *Python 3.6.1*
# 
# -  The Word Count is the canonical MapReduce task where the goal is to count the occurrences of words in a list of documents. 
# -  First we tokenize the document and split text into a list of individual words. (Note: Punctuation is counted as a word in this example). 
# -  The **Mapper** emits key-value pairs of words and individual counts of 1. 
# -  The **Reducer** aggregates each individual count for a word and emits a key-value pair of word and total count of occurrences in all the documents.
# 

# -  In Part 1, we create a MapReduce object that is used to pass data between the map function and the reduce function.

# In[3]:

import MapReduce
import sys

"""
Word Count in the Python MapReduce Framework
Input: (document_id, document_text)
Mapper Output: (word1, 1), (word1, 1)...etc.
Reducer Output: (word, total count)
"""

mr = MapReduce.MapReduce()


# -  In Part 2, the mapper function tokenizes each document and emits a key-value pair. The key is a word formatted as a string and the value is the integer 1 to indicate an occurrence of word.
# 

# In[ ]:

def mapper(record):
    # Input = (key: document id, value: document contents)
    # Output = (key: word, value: 1)
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, 1)


# -  In Part 3, the reducer function sums up the list of occurrence counts and emits a count for word. Since the mapper function emits the integer 1 for each word, each element in the `list_of_values` is the integer 1.
# 
# -  The list of occurrence counts is summed and a `(word, total)` tuple is emitted where word is a string and total is an integer.

# In[4]:

def reducer(key, list_of_values):
    # Input = (key: word, value: 1)
    # Output = (key: word, value: total count of occurrences)  
    total = 0
    for v in list_of_values:
        total += v
    mr.emit((key, total))


# -  In Part 4, the code loads the json file and executes the MapReduce query which prints the result to stdout.

# In[ ]:

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

