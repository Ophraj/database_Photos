class Photo:
    def __init__(self, photoID=None, description=None, size=None):
        self.__photoID = photoID
        self.__description = description
        self.__size = size

    def getPhotoID(self):
        return self.__photoID

    def setPhotoID(self, photoID):
        self.__photoID = photoID

    def getDescription(self):
        return self.__description

    def setDescription(self, description):
        self.__description = description

    def getSize(self):
        return self.__size

    def setSize(self, size):
        self.__size = size

    @staticmethod
    def badPhoto():
        return Photo()

    def __str__(self):
        return "photoID=" + str(self.__photoID) + ", description=" + str(self.__description) + ", size=" + str(self.__size)
