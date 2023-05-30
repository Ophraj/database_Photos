class Disk:
    def __init__(self, diskID=None, company=None, speed=None, free_space=None, cost=None):
        self.__diskID = diskID
        self.__company = company
        self.__speed = speed
        self.__free_space = free_space
        self.__cost = cost

    def getDiskID(self):
        return self.__diskID

    def setDiskID(self, diskID):
        self.__diskID = diskID

    def getCompany(self):
        return self.__company

    def setCompany(self, company):
        self.__company = company

    def getSpeed(self):
        return self.__speed

    def setSpeed(self, speed):
        self.__speed = speed

    def getFreeSpace(self):
        return self.__free_space

    def setFreeSpace(self, free_space):
        self.__free_space = free_space

    def getCost(self):
        return self.__cost

    def setCost(self, cost):
        self.__cost = cost

    @staticmethod
    def badDisk():
        return Disk()

    def __str__(self):
        return "DiskID=" + str(self.__diskID) + ", company=" + str(self.__company) + ", speed=" + str(
            self.__speed) + ", free space=" + str(self.__free_space) + ", cost=" + str(self.__cost)
