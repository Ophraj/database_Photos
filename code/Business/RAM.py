class RAM:
    def __init__(self, ramID=None, company=None, size=None):
        self.__ramID = ramID
        self.__company = company
        self.__size = size

    def getRamID(self):
        return self.__ramID

    def setRamID(self, ramID):
        self.__ramID = ramID

    def getCompany(self):
        return self.__company

    def setCompany(self, company):
        self.__company = company

    def getSize(self):
        return self.__size

    def setSize(self, size):
        self.__size = size

    @staticmethod
    def badRAM():
        return RAM()

    def __str__(self):
        return "RamID=" + str(self.__ramID) + ", company=" + str(self.__company) + ", size=" + str(self.__size)
