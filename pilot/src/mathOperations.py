import locale
import json
class MathOperations:
#
### Define class variables here
#
    numOfDaysperMonth = 20
    numOfMonths = 12
    ratePerDay = 650.00
    name = 'Mich'

    def testAddition (self,x,y):
        return x + y

    def testMultiplication (self,a,b):
        return a * b

    def dispName(self):
        print("\nmy name is " + self.name)

    def expectedYearlyIncome(self):
        locale.setlocale(locale.LC_ALL, "en_GB")
        # self.<> are instance variables
        return locale.currency(self.numOfDaysperMonth * self.numOfMonths * self.ratePerDay)
        # return str(self.numOfDaysperMonth * self.numOfMonths * self.ratePerDay)

    def returnDailyRate(self):
        return self.ratePerDay


class Jsonstuff:

    # a Python object here
    currencies = {
        "YEN":"(Japanese Yen)",
        "USD":"(US Dollar)",
        "EUR":"(Euro)",
        "GBP":"(Great Britain Pound)"
    }

    def loadJson (self):
        # convert into json
        return json.dumps(self.currencies)
