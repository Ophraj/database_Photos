from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.DBConnector import ResultSet
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = "BEGIN TRANSACTION; " \
            f"CREATE TABLE Photos " \
            f"(" \
            f"photo_id INTEGER, " \
            f"description TEXT NOT NULL, " \
            f"size INTEGER NOT NULL, " \
            f"PRIMARY KEY(photo_id), " \
            f"UNIQUE(photo_id), " \
            f"CHECK(photo_id>0), " \
            f"CHECK(size>=0)" \
            f");"
        '''query = '''
        '''CREATE TABLE Photos
        (
        photo_id INTEGER, 
        description TEXT NOT NULL, 
        size INTEGER NOT NULL, 
        PRIMARY KEY(photo_id),
        UNIQUE(photo_id),
        CHECK(photo_id>0),
        CHECK(size>=0)
        ); 
        '''
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
        
    pass


def clearTables():
    pass


def dropTables():
    pass


def addPhoto(photo: Photo) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Photos VALUES({photoID}, {photoDescription}, {photoSize});").format(
                                                                photoID=sql.Literal(photo.getPhotoID()),
                                                                photoDescription=sql.Literal(photo.getDescription()),
                                                                photoSize=sql.Literal(photo.getSize()))

        conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()
    return ReturnValue.OK


def getPhotoByID(photoID: int) -> Photo:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Photos WHERE photo_id={photoID};").format(
            photoID=sql.Literal(photoID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Photo.badPhoto()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Photo.badPhoto()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Photo.badPhoto()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Photo.badPhoto()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Photo.badPhoto()
    except Exception as e:
        print(e)
        return Photo.badPhoto()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if rows_effected == 0:
        return Photo.badPhoto()
    else:
        photo_entry = result.rows[0]
        print(photo_entry)
        return Photo(photo_entry[0], photo_entry[1], photo_entry[2])


def deletePhoto(photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    return ReturnValue.OK


def addPhotoToDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    return 0


def getTotalRamOnDisk(diskID: int) -> int:
    return 0


def getCostForDescription(description: str) -> int:
    return 0


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    return True


def getDisksContainingTheMostData() -> List[int]:
    return []


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getClosePhotos(photoID: int) -> List[int]:
    return []
