
# coding: utf-8

# ## MapReduce in Python
# ### Exercise 1: Inverted Index

# *Python 3.6.1*

# -  An inverted index is a primitive method for text retrieval. You can think of the functionality of a search engine where you wish to search with keywords and get a result of a list of documents containing those words.
# 
# #### Mapper Input
# -  The input for this task is a JSON file containing 2-element lists of `[document_id, document_text]` objects where each attribute is a String type. <br />
# -  Any type of sets of documents can be used, but in this example I am using the books.JSON file containing a list of of book titles and the text of that book. <br />
# 
# #### Reducer Output
# -  The output should be a `[word, [document_ID list]]` tuple where word is a String and document ID list is a list of Strings.

# In[6]:

import MapReduce
import sys

"""
Inverted Index in the Python MapReduce Framework
Input: (document_id, document_text)
Mapper Output: (word1, document_id1), (word1, document_id2)...etc.
Reducer Output: (word, [list of document_id's])
"""

mr = MapReduce.MapReduce() 


# In[3]:

def mapper(record):
    # Input = (key: document id, value: document text)
    # Output = (key: word, value: document id)
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, record[0])


# In[4]:

def reducer(key, list_of_values):
    # Input = (key: word, value: document id)
    # Output = (key: word, value: list of document ids)
    list_docs = []
    for v in list_of_values:
        list_docs.append(v)
    mr.emit((key, list_docs))


# In[ ]:

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

