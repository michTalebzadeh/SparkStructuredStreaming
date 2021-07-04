class MathOperations:
    @staticmethod
    def testAddition (x, y):
        return x + y

    @staticmethod
    def testMultiplication (a, b):
        return a * b

tmp = MathOperations
print ("\nThe sum is " + str(tmp.testAddition(2,3)))
print ("\nThe multiplication is " + str(tmp.testMultiplication(10,12)))
