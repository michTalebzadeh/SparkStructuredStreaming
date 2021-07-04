import sys
def format_attributes(*args, **attributes):
  for a in args:
     print(a)
  for kw in attributes:
    print(kw,attributes(kw))

if __name__== "__main__":
    format_attributes(int(sys.argv[1]), int(sys.argv[2]), sys.argv[3], sys.argv[4], sys.argv[5])

##run like python3 ./format_attributes.py  1 2 3 {'a':1,'b':'albatros'}
