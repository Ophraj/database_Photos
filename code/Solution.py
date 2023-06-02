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
        create_query = f"""
                    BEGIN;
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
    conn = None
    try:
        conn = Connector.DBConnector()

        # Create the tables in one transaction to create
        create_query = f"""
                        BEGIN;
                        DELETE FROM Photos CASCADE;   
                        DELETE FROM Disks CASCADE;
                        DELETE FROM RAMs CASCADE;
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
    pass


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        create_query = f"""
                            BEGIN;
                            DROP TABLE IF EXISTS Photos CASCADE;   
                            DROP TABLE IF EXISTS Disks CASCADE;
                            DROP TABLE IF EXISTS RAMs CASCADE;
                            DROP TABLE IF EXISTS StoredOn CASCADE;
                            DROP TABLE IF EXISTS PartOf CASCADE;
                            DROP VIEW IF EXISTS Photos_Stored_On_Disks;
                            DROP VIEW IF EXISTS Rams_Part_Of_Disks;
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
    pass


def addPhoto(photo: Photo) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""BEGIN;
                        INSERT INTO Photos VALUES({photoID}, 
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
        query = sql.SQL("""BEGIN;
                        SELECT * FROM Photos WHERE photo_id={photoID};""").format(
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
        query = sql.SQL("""
                    BEGIN;
                    UPDATE Disks
                    SET free_space = free_space - {photoSize}
                    WHERE disk_id IN (SELECT disk_id
                                      FROM StoredOn
                                      WHERE photo_id = {photoID});
                    DELETE
                    FROM Photos
                    WHERE photo_id = {photoID}};
                    """).format(photoID=sql.Literal(photo.getPhotoID()),
                                photoSize=sql.Literal(photo.getSize()))

        rows_effected, result = conn.execute(query)
        conn.commit()
        # TODO: check if the return value are in right order (ok in case of 'not exist' and error else)

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

    if rows_effected == 0:
        conn.rollback()

    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""BEGIN;
                        INSERT INTO Disks VALUES({diskID}, {diskCompany}, 
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
        query = sql.SQL("""BEGIN;
                        SELECT * FROM Disks WHERE disk_id={diskID};""").format(
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
        query = sql.SQL("""BEGIN;
                        DELETE
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
        query = sql.SQL("""
                        BEGIN;
                        INSERT INTO RAMs 
                        VALUES({ramID}, {ramSize}, {ramCompany});""").format(ramID=sql.Literal(ram.getRamID()),
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
        query = sql.SQL("""BEGIN;
                        SELECT * FROM RAMs WHERE ram_id={RamId};""").format(
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
        query = sql.SQL("""BEGIN;
                            DELETE
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
        query = sql.SQL(""" BEGIN;
                        INSERT INTO Photos VALUES({photoID}, 
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
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        BEGIN TRANSACTION; 
                        UPDATE Disks 
                        SET free_space = free_space - {photoSize} 
                        WHERE disk_id={diskID};

                        INSERT INTO StoredOn 
                        VALUES ({photoID}, {diskID});

                        COMMIT;
                        """).format(photoID=sql.Literal(photo.getPhotoID()),
                                    photoSize=sql.Literal(photo.getSize()),
                                    diskID=sql.Literal(diskID))

        conn.execute(query)
        conn.commit()

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.BAD_PARAMS
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
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


def removePhotoFromDisk(photo: Photo, diskID: int) -> ReturnValue:

    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        BEGIN TRANSACTION; 
                        DELETE FROM StoredOn 
                        WHERE photo_id={photoID} AND disk_id={diskID};
                         
                        UPDATE Disks
                        SET free_space = free_space - {photoSize}
                        WHERE disk_id = {diskID};
                        
                        COMMIT;
                        """).format(photoID=sql.Literal(photo.getPhotoID()),
                                    photoSize=sql.Literal(photo.getSize()),
                                    diskID=sql.Literal(diskID)
                                    )

        conn.execute(query)
        conn.commit()
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
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


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            INSERT INTO PartOf 
                            VALUES ({ramID}, {diskID});
                            """).format(ramID=sql.Literal(ramID),
                                        diskID=sql.Literal(diskID)
                                        )

        conn.execute(query)
        conn.commit()

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
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


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            DELETE FROM PartOf 
                            WHERE ram_id={ramID} AND disk_id={diskID} 
                            """).format(ramID=sql.Literal(ramID),
                                        diskID=sql.Literal(diskID)
                                        )

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.UNIQUE_VIOLATION as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        conn.rollback()
        return ReturnValue.ERROR
    except DatabaseException.ConnectionInvalid as e:
        conn.rollback()
        print(e)
        return ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
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
            return ReturnValue.NOT_EXISTS

        conn.close()

    return ReturnValue.OK


def averagePhotosSizeOnDisk(diskID: int) -> float:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        SELECT AVG(size) 
                        FROM Photos_Stored_On_Disks
                        WHERE disk_id = {diskID}
                        """).format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    average_entry = result.rows[0]
    average_result = average_entry[0]
    print(average_entry)
    if result.isEmpty() or average_result is None:
        return 0

    return average_result


def getTotalRamOnDisk(diskID: int) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            SELECT SUM(size) 
                            FROM Rams_Part_Of_Disks
                            WHERE disk_id = {diskID};
                            """).format(diskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    total_entry = result.rows[0]
    total_result = total_entry[0]
    print(total_entry)
    if result.isEmpty() or total_result is None:
        return 0

    return total_result


def getCostForDescription(description: str) -> int:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                                SELECT SUM(cost*size) 
                                FROM Photos_Stored_On_Disks pd INNER JOIN Disks d ON pd.disk_id = d.disk_id
                                WHERE description = {description};
                                """).format(description=sql.Literal(description))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return -1
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return -1
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return -1
    except Exception as e:
        print(e)
        return -1
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    entry = result.rows[0]
    entry_result = entry[0]
    print(entry)
    if result.isEmpty() or entry_result is None:
        return 0

    return entry_result


def getPhotosCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""SELECT photo_id 
            FROM Photos, (SELECT free_space FROM Disks where Disks.disk_id={DiskID}) AS Free_Space
            WHERE size <= Free_Space.free_space
            ORDER BY photo_id DESC
            LIMIT 5
            """).format(DiskID=sql.Literal(diskID))
        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    photo_ids = [row[0] for row in result.rows]
    return photo_ids


def getPhotosCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""SELECT photo_id
                            FROM Photos,
                            (SELECT free_space FROM Disks WHERE Disks.disk_id={DiskID}) AS Free_Space,
                            (SELECT SUM(size) as sum_ram FROM Rams_Part_Of_Disks WHERE Rams_Part_Of_Disks.disk_id={DiskID}) AS Sum_RAMs
                            WHERE size<= Free_Space.free_space AND ((Sum_RAMs.sum_ram IS NOT NULL AND size <= Sum_RAMs.sum_ram) OR Sum_RAMs.sum_ram IS NULL)
                            ORDER by photo_id ASC
                            LIMIT 5;""").format(DiskID=sql.Literal(diskID))

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    photo_ids = [row[0] for row in result.rows]
    return photo_ids


def isCompanyExclusive(diskID: int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        SELECT company 
                        FROM Disks
                        WHERE disk_id = {DiskID} AND company = ALL(SELECT company FROM Rams_Part_Of_Disks WHERE disk_id={DiskID})
                        """).format(DiskID=sql.Literal(diskID))

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return False
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return False
    except Exception as e:
        print(e)
        return False
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if result.isEmpty():
        return False

    return True


def isDiskContainingAtLeastNumExists(description : str, num : int) -> bool:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            SELECT disk_id 
                            FROM Photos_Stored_On_Disks 
                            WHERE description ={description} 
                            GROUP BY disk_id 
                            HAVING COUNT(*)>= {num};
                            """).format(description=sql.Literal(description), num=sql.Literal(num))

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return False
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return False
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return False
    except Exception as e:
        print(e)
        return False
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    if result.isEmpty():
        return False

    return True


def getDisksContainingTheMostData() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                        SELECT d.disk_id, SUM(pd.size) as amount_data
                        FROM Photos_Stored_On_Disks pd RIGHT OUTER JOIN Disks d ON pd.disk_id = d.disk_id
                        GROUP BY d.disk_id
                        ORDER BY amount_data DESC, d.disk_id ASC
                        LIMIT 5;
                        """)

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    disk_ids = [row[0] for row in result.rows]
    return disk_ids


def getConflictingDisks() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                            SELECT DISTINCT so1.disk_id
                            FROM StoredOn so1 JOIN StoredOn so2 ON so1.photo_id = so2.photo_id AND so1.disk_id <> so2.disk_id
                            ORDER BY disk_id ASC;
                            """)

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    disk_ids = [row[0] for row in result.rows]
    return disk_ids


def mostAvailableDisks() -> List[int]:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("""
                                SELECT d.disk_id, d.speed, COUNT(*) as num_photos
                                FROM Photos_Stored_On_Disks pd RIGHT OUTER JOIN Disks d ON pd.disk_id = d.disk_id
                                GROUP BY d.disk_id
                                ORDER BY num_photos DESC, d.speed DESC, d.disk_id ASC
                                LIMIT 5;
                                """)

        rows_effected, result = conn.execute(query)
        conn.commit()

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.ConnectionInvalid as e:
        print(e)
        return []
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
        return []
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
        return []
    except Exception as e:
        print(e)
        return []
    finally:
        # will happen any way after try termination or exception handling
        conn.close()

    disk_ids = [row[0] for row in result.rows]
    return disk_ids


def getClosePhotos(photoID: int) -> List[int]:
    return []