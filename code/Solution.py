from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()

        # Create the tables in one transaction to create
        conn.execute("BEGIN;")
        create_query = f"""
                    CREATE TABLE Photos ( 
                    photo_id INTEGER,
                    description TEXT NOT NULL,
                    size INTEGER NOT NULL,
                    PRIMARY KEY (photo_id),
                    UNIQUE (photo_id),
                    CHECK (photo_id > 0),
                    CHECK (size >= 0)
                );   
                    CREATE TABLE Disks (
                    disk_id INTEGER,
                    company TEXT NOT NULL,
                    speed INTEGER NOT NULL,
                    free_space INTEGER NOT NULL,
                    cost INTEGER NOT NULL,
                    PRIMARY KEY (disk_id),
                    UNIQUE (disk_id),
                    CHECK (disk_id > 0),
                    CHECK (speed > 0),
                    CHECK (free_space >= 0),
                    CHECK (cost > 0)
                ); 
                    CREATE TABLE RAMs (
                    ram_id INTEGER, 
                    size INTEGER NOT NULL,
                    company TEXT NOT NULL, 
                    PRIMARY KEY(ram_id),
                    UNIQUE(ram_id),
                    CHECK(ram_id>0),
                    CHECK(size>0)
                ); 
                    CREATE TABLE StoredOn (
                    photo_id INTEGER, 
                    disk_id INTEGER,
                    PRIMARY KEY(photo_id, disk_id),
                    FOREIGN KEY(photo_id) REFERENCES Photos ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY(disk_id) REFERENCES Disks ON DELETE CASCADE ON UPDATE CASCADE
                ); 
                    CREATE TABLE PartOf(
                    ram_id INTEGER, 
                    disk_id INTEGER,
                    PRIMARY KEY(ram_id, disk_id),
                    FOREIGN KEY(ram_id ) REFERENCES RAMs ON DELETE CASCADE ON UPDATE CASCADE,
                    FOREIGN KEY(disk_id) REFERENCES Disks ON DELETE CASCADE ON UPDATE CASCADE
                ); 
                CREATE VIEW Photos_Stored_On_Disks AS
                    SELECT disk_id, StoredOn.photo_id, description, size 
                    FROM Photos INNER JOIN StoredOn
                    ON Photos.photo_id = StoredOn.photo_id;

                    CREATE VIEW Rams_Part_Of_Disks AS
                    SELECT disk_id, PartOf.ram_id, size, company
                    FROM RAMs INNER JOIN PartOf
                    ON RAMs.ram_id = PartOf.ram_id;
                """

        conn.execute(create_query)
        conn.commit()
        # Commit if all the queries succeeded
    except Exception as e:
        conn.rollback()
        # Roll back if one of the query failed
        print(e)
    finally:
        conn.close()


def clearTables():
    pass


def dropTables():
    pass


def addPhoto(photo: Photo) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
        query = sql.SQL("""INSERT INTO Photos VALUES({photoID}, 
                        {photoDescription}, {photoSize});""").\
            format(photoID=sql.Literal(photo.getPhotoID()),
                   photoDescription=sql.Literal(photo.getDescription()),
                   photoSize=sql.Literal(photo.getSize()))

        conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return ReturnValue.OK


def getPhotoByID(photoID: int) -> Photo:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
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
    conn = None

    try:
        conn = Connector.DBConnector()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM Photos")
        initial_count = cursor.fetchone()[0]

        conn.execute("BEGIN;")
        query = sql.SQL("""UPDATE Disks
                    SET free_space = free_space - {photoSize}
                    WHERE disk_id IN (SELECT disk_id
                                      FROM StoredOn
                                      WHERE photo_id = {photoID});
                    DELETE
                    FROM Photos
                    WHERE photo_id = {photoID}};
                    """).format(photoID=sql.Literal(photo.getPhotoID()),
                                photoSize=sql.Literal(photo.getSize()))

        conn.execute(query)
        conn.commit()
        # TODO: check if the return value are in right order (ok and then error)
        cursor.execute("SELECT COUNT(*) FROM Photos")
        final_count = cursor.fetchone()[0]
        deleted_photos = initial_count - final_count

        if deleted_photos != 1:
            conn.rollback()
            return ReturnValue.OK

    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
        query = sql.SQL("""INSERT INTO Disks VALUES({diskID}, {diskCompany}, 
                        {diskSpeed}, {diskFreeSpace}, {diskCost});"""). \
            format(diskID=sql.Literal(disk.getDiskID()),
                   diskCompany=sql.Literal(disk.getCompany()),
                   diskSpeed=sql.Literal(disk.getSpeed()),
                   diskFreeSpace=sql.Literal(disk.getFreeSpace()),
                   diskCost=sql.Literal(disk.getCost()))

        conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return ReturnValue.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
        query = sql.SQL("SELECT * FROM Disks WHERE disk_id={diskID};").format(
            diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return Disk.badDisk()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return Disk.badDisk()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return Disk.badDisk()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return Disk.badDisk()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return Disk.badDisk()
    except Exception as e:
        print(e)
        return Disk.badDisk()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if rows_effected == 0:
        return Disk.badDisk()
    else:
        disk_entry = result.rows[0]
        print(disk_entry)
        return Disk(disk_entry[0], disk_entry[1], disk_entry[2], disk_entry[3], disk_entry[4])


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()

        conn.execute("BEGIN;")
        query = sql.SQL("""DELETE
                        FROM Disks
                        WHERE disk_id = {disk_ID};
                        """).format(disk_ID=sql.Literal(diskID))

        rows_effected, result = conn.execute(query)
        conn.commit()
        if rows_effected != 1:
            conn.rollback()
            return ReturnValue.NOT_EXISTS

    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
        query = sql.SQL("""INSERT INTO RAMs VALUES({ramID}, {ramCompany}, 
                            {ramSize});""").format(ramID=sql.Literal(ram.getRamID()),
                                                   ramCompany=sql.Literal(ram.getCompany()),
                                                   ramSize=sql.Literal(ram.getSize()))

        conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    return ReturnValue.OK


def getRAMByID(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
        query = sql.SQL("SELECT * FROM RAMs WHERE ram_id={RamId};").format(
            RamId=sql.Literal(ramID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return RAM.badRAM()
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return RAM.badRAM()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return RAM.badRAM()
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return RAM.badRAM()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return RAM.badRAM()
    except Exception as e:
        print(e)
        return RAM.badRAM()
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if rows_effected == 0:
        return RAM.badRAM()
    else:
        ram_entry = result.rows[0]
        print(ram_entry)
        return RAM(ram_entry[0], ram_entry[1], ram_entry[2])


def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    rows_effected, result = 0, Connector.ResultSet()
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
        query = sql.SQL("""DELETE
                            FROM RAMs
                            WHERE ram_id = {RamID};
                            """).format(RamID=sql.Literal(ramID))

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        if rows_effected != 1:
            conn.rollback()
            return ReturnValue.NOT_EXISTS

        conn.close()

    return ReturnValue.OK


def addDiskAndPhoto(disk: Disk, photo: Photo) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("BEGIN;")
        query = sql.SQL("""INSERT INTO Photos VALUES({photoID}, 
                                {photoDescription}, {photoSize});
                            INSERT INTO Disk VALUES({diskID}, {diskCompany}, 
                                {diskSpeed}, {diskFreeSpace}, {diskCost});
                            """).\
            format(photoID=sql.Literal(photo.getPhotoID()),
                   photoDescription=sql.Literal(photo.getDescription()),
                   photoSize=sql.Literal(photo.getSize()),
                   diskID=sql.Literal(disk.getDiskID()),
                   diskCompany=sql.Literal(disk.getCompany()),
                   diskSpeed=sql.Literal(disk.getSpeed()),
                   diskFreeSpace=sql.Literal(disk.getFreeSpace()),
                   diskCost=sql.Literal(disk.getCost()))

        conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except Exception as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

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