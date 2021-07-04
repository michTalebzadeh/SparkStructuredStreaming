# you can define a constructor of the class and then the function is bound.
# Essentially internal methods can and should operate on internal variables only (encapsulation). 
#
class MathOperations:
    def __init__ (self,x,y):
       self.y = y
       self.x = x
    def testAddition (self):
        return self.x + self.y

tmp = MathOperations(2,3)
print tmp.testAddition()
