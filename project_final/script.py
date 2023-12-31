#!/usr/bin/python3.6

'''
    test program
'''

from subprocess import Popen, PIPE
import os
from random import shuffle
from timeit import default_timer as timer
from math import log, floor
from time import sleep
import struct

INSERT_CMD_FMTS = "i %d %ld %s\n"
DELETE_CMD_FMTS = "d %d %ld\n"
UPDATE_CMD_FMTS = "u %d %ld %s\n"
FIND_CMD_FMTS = "f %d %ld\n"
QUIT_CMD_FMTS = "q\n"
OPEN_FMT_STR = "o %s\n"
FIND_RESULT_FMTS = "Key: %ld, Value: %s"
NOT_FOUND_RESULT = "Not Exists"
TRX_BEGIN = "b\n"
TRX_COMMIT = "c\n"
TRX_ABORT = "a\n"
RESULT_FMTS = "Result: %d/%d (%.2f) %.2f secs"

TARGET_DB_NAME = 'DATA1'
BACKUP_DB_NAME = 'DATA1.bak'
LAST_TEST_DB_NAME = 'backup.db'
EXECUTABLE_NAME = 'test'


SMALL_CASE = 2 ** 10
MEDIUM_CASE = 2 ** 15
LARGE_CASE = 2 ** 20

def test_case(arr):
    os.system('rm -f ' + TARGET_DB_NAME)
    sleep(0.1)
    f = open("log_script.txt", "w")
    p = Popen("./" + EXECUTABLE_NAME, stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;

    # insert start
    start = timer()
    p.stdin.write((OPEN_FMT_STR % (TARGET_DB_NAME)).encode('utf-8'))
    f.write(OPEN_FMT_STR % (TARGET_DB_NAME))
    p.stdin.flush()
    tableNum = int(p.stdout.readline().decode('utf-8').strip())

    for i in arr:
        input_d = INSERT_CMD_FMTS % (tableNum, i, 'a' + str(i))
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()

    end = timer()

    for i in arr:
        input_d = FIND_CMD_FMTS % (tableNum, i)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == (FIND_RESULT_FMTS % (i, 'a' + str(i))).strip()):
            succ += 1

    p.stdin.write(QUIT_CMD_FMTS.encode('utf-8'))
    p.stdin.flush()
    f.close()

    os.system("cp " + TARGET_DB_NAME + " " + LAST_TEST_DB_NAME)

    return succ, end - start

def test_case_update(arr):
    os.system('cp ' + LAST_TEST_DB_NAME + ' ' + TARGET_DB_NAME)
    sleep(0.1)
    f = open("log_script.txt", "w")
    p = Popen("./" + EXECUTABLE_NAME, stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;

    start = timer()
    p.stdin.write((OPEN_FMT_STR % (TARGET_DB_NAME)).encode('utf-8'))
    f.write(OPEN_FMT_STR % (TARGET_DB_NAME))
    p.stdin.flush()
    tableNum = int(p.stdout.readline().decode('utf-8').strip())

    aborted = []
    committed = []
    k = 0
    i = 0
    size = len(arr)
    while(i < size):
        input_d = TRX_BEGIN
        f.write(TRX_BEGIN)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        if k%2 == 0:
            for j in range(10):
                aborted.append(arr[i])
                input_d = UPDATE_CMD_FMTS % (tableNum, arr[i], 'b' + str(arr[i]))
                f.write(input_d)
                p.stdin.write(input_d.encode("utf-8"))
                p.stdin.flush()
                i += 1
            input_d = TRX_ABORT
            f.write(TRX_ABORT)
            p.stdin.write(input_d.encode("utf-8"))
            p.stdin.flush()
        else:
            for j in range(10):
                committed.append(arr[i])
                input_d = UPDATE_CMD_FMTS % (tableNum, arr[i], 'b' + str(arr[i]))
                f.write(input_d)
                p.stdin.write(input_d.encode("utf-8"))
                p.stdin.flush()
                i+= 1
            input_d = TRX_COMMIT
            f.write(TRX_COMMIT)
            p.stdin.write(input_d.encode("utf-8"))
            p.stdin.flush()
        k += 1
    end = timer()

    p.stdin.write(QUIT_CMD_FMTS.encode('utf-8'))
    p.stdin.flush()
    p.wait(timeout=None)

    p = Popen("./" + EXECUTABLE_NAME, stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;

    p.stdin.write((OPEN_FMT_STR % (TARGET_DB_NAME)).encode('utf-8'))
    p.stdin.flush()
    tableNum = int(p.stdout.readline().decode('utf-8').strip())

    for i in aborted:
        input_d = FIND_CMD_FMTS % (tableNum, i)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        revresult = (FIND_RESULT_FMTS % (i, 'a' + str(i))).strip()
        result = p.stdout.readline().decode("utf-8").strip()

        if (result == revresult):
            succ += 1
        else:
            print("rev result : " + revresult)
            print("result : " + result)

    for i in committed:
        input_d = FIND_CMD_FMTS % (tableNum, i)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        revresult = (FIND_RESULT_FMTS %(i, 'b' + str(i))).strip()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == revresult):
            succ += 1
        else:
            print("rev result : " + revresult)
            print("result : " + result)

    p.stdin.write(QUIT_CMD_FMTS.encode('utf-8'))
    p.stdin.flush()
    f.close()
    return succ, end - start

def test_update_seq(casename, case_size):
    print(casename + " Test")

    result, elapse = test_case_update(range(case_size))

    print(RESULT_FMTS % (result, case_size, float(result)/(case_size) * 100, elapse))
    if (result != case_size):
        print("FAILED - Stop Testing")
        exit()
    return 0
def test_update_rnd(casename, case_size):
    print(casename + " Test")
    arr = list(range(case_size))
    shuffle(arr)

    result, elapse = test_case_update(arr)
    print(RESULT_FMTS % (result, case_size, float(result)/(case_size) * 100, elapse))
    if (result != case_size):
        print("FAILED - Stop Testing")
        exit()
    return 0

def test_case_seq(casename, case_size):
    print(casename + " Test")

    result, elapse = test_case(range(case_size))

    print(RESULT_FMTS % (result, case_size, float(result)/case_size * 100, elapse))
    if (result != case_size):
        print("FAILED - Stop Testing")
        exit()



def test_case_rnd(casename, case_size):
    print(casename + " Test")
    arr = list(range(case_size))
    shuffle(arr)

    result, elapse = test_case(arr)

    print(RESULT_FMTS % (result, case_size, float(result)/case_size * 100, elapse))
    if (result != case_size):
        print("FAILED - Stop Testing")
        exit()

def test_delete(remain_rec, delete_rec):
    os.system('rm -f ' + TARGET_DB_NAME)
    os.system('cp ' + BACKUP_DB_NAME + ' ' + TARGET_DB_NAME)
    sleep(0.1)
    f = open("log_script.txt", "w")
    p = Popen("./" + EXECUTABLE_NAME, stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;

    # insert start
    start = timer()

    p.stdin.write((OPEN_FMT_STR % (TARGET_DB_NAME)).encode('utf-8'))
    f.write(OPEN_FMT_STR % (TARGET_DB_NAME))
    p.stdin.flush()
    tableNum = int(p.stdout.readline().decode('utf-8').strip())

    for i in delete_rec:
        input_d = DELETE_CMD_FMTS % (tableNum, i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))

        p.stdin.flush()

    end = timer()

    remain_rec.sort()
    delete_rec.sort()
    for i in remain_rec:
        input_d = FIND_CMD_FMTS % (tableNum, i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == (FIND_RESULT_FMTS % (i, 'a' + str(i))).strip()):
            succ += 1

    for i in delete_rec:
        input_d = FIND_CMD_FMTS % (tableNum, i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == NOT_FOUND_RESULT):
            succ += 1

    p.stdin.write(QUIT_CMD_FMTS.encode('utf-8'))
    p.stdin.flush()
    f.close()

    return succ, end - start

def test_delete_random(casename, pick):
    print(casename + " Test")
    arr = list(range(LARGE_CASE))
    shuffle(arr)

    result, elapse = test_delete(arr[pick:], arr[:pick])
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED - Stop Testing")
        exit()

def test_delete_seq():
    print("Delete All Records Sequantial")
    arr = list(range(LARGE_CASE))

    result, elapse = test_delete([], arr)
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED Delete Seq - Stop Testing")
        exit()

def test_delete_rev():
    print("Delete All Records Reversal")
    arr = list(range(LARGE_CASE))
    arr.reverse()

    result, elapse = test_delete([], arr)
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED - Stop Testing")
        exit()

def test_delete_chunk():
    print("Delete Chunk Records randomly")
    arr = list(range(LARGE_CASE))
    exp = floor(log(LARGE_CASE, 2) / 2)
    start = 2 ** exp
    end = 2 ** (exp+1)

    deleted = arr[start: end]
    arr = arr[:start] + arr[end:]

    result, elapse = test_delete(arr, deleted)
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))
    if (result != LARGE_CASE):
        print("FAILED - Stop Testing")
        exit();


def make_backup():
    os.system('cp ' + LAST_TEST_DB_NAME + ' ' + BACKUP_DB_NAME)

os.system('make clean > /dev/null')
os.system('make > /dev/null')

try:
    os.remove(TARGET_DB_NAME)
except:
    pass

print("-------------- Sequantial Insert Test --------------")
#test_case_seq("Small(2^10)", SMALL_CASE)
#test_case_seq("Medium(2^15)", MEDIUM_CASE)
test_case_seq("INSERT", 2**12)
#test_case_seq("Medium(2^15)", MEDIUM_CASE)
#test_case_seq("Large(2^20)", LARGE_CASE)
# test_update_seq("UPDATE", 100000)
# test_update_rnd("UPDATE", 10000)

#print("--------------   Random Insert Test   --------------")
#test_case_rnd("Small(2^10)", SMALL_CASE)
#test_case_rnd("Medium(2^15)", MEDIUM_CASE)
#test_case_rnd("Large(2^20)", LARGE_CASE)

#sleep(0.1)
#make_backup()

#print("--------------      Delete Test       --------------")
#test_delete_seq()
#test_delete_rev()
#test_delete_chunk()
#test_delete_random("Random_Delete_Small(2^10)", SMALL_CASE)
#test_delete_random("Random_Delete_Medium(2^15)", MEDIUM_CASE)
#test_delete_random("Random_Delete_Large(2^19)", int(LARGE_CASE / 2))
#test_delete_random("Random_Delete_ALL(2^20)", LARGE_CASE)
