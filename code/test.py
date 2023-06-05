import os
import sys
import inspect

import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest
from Business.Photo import Photo
from Business.RAM import RAM
from Business.Disk import Disk

if __name__ == '__main__':
    print("FIRST: Drop everything previous")
    Solution.dropTables()

    print("0. Creating all tables")
    Solution.createTables()

    photos = [Photo(i, "Tree", i*10) for i in range(1, 10)]
    disks = [Disk(1,"MC", 23, 345, 12) for i in range(1, 10)]

    disk1 = Disk(1,"MC", 23, 345, 12)
    disk2 = Disk(2, "MC", 23, 345, 12)
    ram1 = RAM(1,"MC", 30)
    ram2 = RAM(2, "MC", 50)
    ram3 = RAM(3, "APP", 50)
    print("1. Add photo with ID 1 and description Tree and size 10")
    Solution.addPhoto(photo1)
    print("2. Add photo with ID 2 and description Tree and size 100")
    Solution.addPhoto(photo2)
    print("3. Getting photo with ID 1")
    photo_result = Solution.getPhotoByID(1)
    print(photo_result)
    print("4. Add Disk with ID 1")
    Solution.addDisk(disk1)
    print("5. Add Disk with ID 2")
    Solution.addDisk(disk2)
    print("6. Add Disk with ID 1 AGAIN")
    Solution.addDisk(disk1)
    print("7. Add Photo 1 to Disk 1")
    Solution.addPhotoToDisk(photo1, 1)
    print("8. Add Photo 1 to Disk 1 AGAIN")
    Solution.addPhotoToDisk(photo1, 1)
    print("9. Remove Photo 1 from Disk 1")
    Solution.removePhotoFromDisk(photo1, 1)
    print("10. Add Photo 1 to Disk 1")
    Solution.addPhotoToDisk(photo1, 1)
    print("11. Add Photo 2 to Disk 1")
    Solution.addPhotoToDisk(photo2, 1)
    print("12. Print average size of photos on Disk 1")
    average = Solution.averagePhotosSizeOnDisk(1)
    print(average)
    print("13. Print average size of photos on Disk 2")
    average = Solution.averagePhotosSizeOnDisk(2)
    print(average)
    print("14. Print average size of photos on Disk 3 that doesn't exist")
    average = Solution.averagePhotosSizeOnDisk(3)
    print(average)
    print("15. Add RAM with ID 1")
    Solution.addRAM(ram1)
    print("16. Add RAM with ID 1 AGAIN")
    Solution.addRAM(ram1)
    print("17. Add RAM with ID 2")
    Solution.addRAM(ram2)
    print("18. Add RAM 1 to Disk 1")
    Solution.addRAMToDisk(1, 1)
    print("18. Add RAM 2 to Disk 1")
    Solution.addRAMToDisk(2, 1)
    print("19. Print total RAM on Disk 1")
    total = Solution.getTotalRamOnDisk(1)
    print(total)
    print("20. Print total RAM on Disk 2 which is empty")
    total = Solution.getTotalRamOnDisk(2)
    print(total)
    print("21. Print total RAM on Disk 3 which doesnt exist")
    total = Solution.getTotalRamOnDisk(3)
    print(total)
    print("22. Print cost for description Tree")
    cost = Solution.getCostForDescription("Tree")
    print(cost)
    print("23. Print cost for description Baba which doesnt exist")
    cost = Solution.getCostForDescription("Baba")
    print(cost)
    print("24. Check if company is exclusive for disk1")
    if Solution.isCompanyExclusive(1):
        print("EXCLUSIVE")
    else:
        print("NOT EXCLUSIVE")
    print("BONUS. Add RAM with ID 3")
    Solution.addRAM(ram3)
    print("25. Add RAM 3 to Disk 1 with a different company")
    Solution.addRAMToDisk(3, 1)
    print("26. Check if company is exclusive for disk1")
    if Solution.isCompanyExclusive(1):
        print("EXCLUSIVE")
    else:
        print("NOT EXCLUSIVE")
    print("27. Check if exist disks that have at least 2 photos with Tree")
    if Solution.isDiskContainingAtLeastNumExists("Tree", 2):
        print("EXIST")
    else:
        print("NOT EXIST")
    print("28. Check if exist disks that have at least 1 photos with Baba")
    if Solution.isDiskContainingAtLeastNumExists("Baba", 1):
        print("EXIST")
    else:
        print("NOT EXIST")
    print("29. Get disks containing most data")
    disk_ids = Solution.getDisksContainingTheMostData()
    print(disk_ids)
    print("30. Add photo with ID 3 and description Tree and size 100")
    Solution.addPhoto(photo3)
    print("31. Add Photo 3 to Disk 1")
    Solution.addPhotoToDisk(photo3, 1)
    print("31. Add Photo 3 to Disk 2")
    Solution.addPhotoToDisk(photo3, 2)
    print("31. Add Photo 1 to Disk 2")
    Solution.addPhotoToDisk(photo1, 2)
    print("32. Get conflicting disks")
    disk_ids = Solution.getConflictingDisks()
    print(disk_ids)
    print("33. Get most")
    disk_ids = Solution.getConflictingDisks()
    print(disk_ids)
    print("FINALLY: Clearing and droping tables")
    Solution.clearTables()
    Solution.dropTables()
