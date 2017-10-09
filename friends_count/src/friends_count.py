
# coding: utf-8

# ## MapReduce in Python
# ### Exercise 2: Friends Count
# 

# *Python 3.6.1*
# 
# -  The task is to count the number of friends for each unique person who is represented by his or her first name. The data is in the friends.json file.
# 
# #### Mapper Input
# 
# -  Each input record is a 2 element list `[personA, personB]` where `personA` is a string representing the name of a person and `personB` is a string representing the name of one of `personA`'s friends. Note that it may or may not be the case that the `personA` is a friend of `personB`.
# 
# #### Reducer Output
# 
# -  The output should be a pair `(person, friend_count)` where `person` is a string and `friend_count` is an integer indicating the number of friends associated with person.

# In[ ]:

import MapReduce
import sys

"""
Friends Count in the Python MapReduce Framework
Input: (personA, personB)
Mapper Output: (personA, 1), (personA, 1)...(personB, 1),...etc.
Reducer Output: (personA, total_friends), (personB, total_friends)
"""

mr = MapReduce.MapReduce()


# -  The output of the Mapper will be a key, value pair of a person's name and a count of 1. The person's name is the **key** ie.only the **left hand** part of the record.

# In[ ]:

def mapper(record):
    # Input = (key: personA, value: personB)
    # Output = (key: person, value: 1)
    key = record[0]
    value = record[1]
    mr.emit_intermediate(key, 1)


# -  The output of the Reducer is a person's name and the aggregated count of friends.

# In[ ]:

def reducer(key, list_of_values):
    # Input = (key: person, value: 1)
    # Output = (key: person, value: total count of friends)  
    total = 0
    for v in list_of_values:
        total += v
    mr.emit((key, total))


# In[ ]:

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)

