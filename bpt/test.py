#!/usr/bin/python3

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
FIND_RESULT_FMTS = "%s\n"
INSERT_CMD_FMTS = "i %d %s\n"
DELETE_CMD_FMTS = "d %d\n"
QUIT_CMD_FMTS = "q\n"
NOT_FOUND_RESULT = "NULL"
FIND_CMD_FMTS = "f %d\n"
RESULT_FMTS = "Result: %d/%d (%.2f) %.2f secs"

SMALL_CASE = 2 ** 10
MEDIUM_CASE = 2 ** 15
LARGE_CASE = 2 ** 20

def test_case(arr):
    f = open("log.txt", "w")
    p = Popen("./main", stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;
    path="o db"
    p.stdin.write(path[0:4].encode("utf-8"))
    #f.write(path[0:4])
    p.stdin.flush()
    # insert start
    start = timer()
    for i in arr:
        input_d = INSERT_CMD_FMTS % (i, 'a' + str(i))
        p.stdin.write(input_d.encode("utf-8"))
        f.write(input_d)
        p.stdin.flush()
    end = timer()
    # validate
    for i in arr:
        input_d = FIND_CMD_FMTS % (i)
        p.stdin.write(input_d.encode("utf-8"))
        f.write(input_d)
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()
        f.write(result+'\n')
        f.write('a' + str(i)+'\n')
        if (result.strip() == ('a' + str(i)+'\n').strip()):
            succ += 1

    p.kill()
    f.close()

    return succ, end - start

def test_case_seq(casename, case_size):
    print(casename + " Test")

    result, elapse = test_case(range(case_size))

    print(RESULT_FMTS % (result, case_size, float(result)/case_size * 100, elapse))
    #os.rename("dbi", "last_dbi")


def test_case_rnd(casename, case_size):
    print(casename + " Test")
    arr = list(range(case_size))
    shuffle(arr)

    result, elapse = test_case(arr)

    print(RESULT_FMTS % (result, case_size, float(result)/case_size * 100, elapse))
    #os.rename("dbi", "last_dbi")

def test_delete(remain_rec, delete_rec):
    f = open("log2.txt", "w")
    p = Popen("./main", stdin=PIPE, stdout=PIPE, shell=True)
    succ = 0;

    path="o db"
    p.stdin.write(path[0:4].encode("utf-8"))

    # insert start
    start = timer()

    for i in delete_rec:
        input_d = DELETE_CMD_FMTS % (i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))

        p.stdin.flush()

    end = timer()

    remain_rec.sort()
    delete_rec.sort()
    for i in remain_rec:
        input_d = FIND_CMD_FMTS % (i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result.strip() == ('a' + str(i)+'\n').strip()):
            succ += 1

    for i in delete_rec:
        input_d = FIND_CMD_FMTS % (i)
        f.write(input_d)
        p.stdin.write(input_d.encode("utf-8"))
        p.stdin.flush()
        result = p.stdout.readline().decode('utf-8').strip()

        if (result == NOT_FOUND_RESULT):
            succ += 1

    p.kill()
    f.close()

    return succ, end - start

def test_delete_random(casename, pick):
    print(casename + " Test")
    arr = list(range(LARGE_CASE))
    shuffle(arr)

    result, elapse = test_delete(arr[pick:], arr[:pick])
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))

def test_delete_seq():
    print("Delete All Records Sequantial")
    arr = list(range(SMALL_CASE))

    result, elapse = test_delete([], arr)
    print(RESULT_FMTS % (result, SMALL_CASE, float(result)/(SMALL_CASE) * 100, elapse))

def test_delete_rev():
    print("Delete All Records Reversal")
    arr = list(range(LARGE_CASE))
    arr.reverse()

    result, elapse = test_delete([], arr)
    print(RESULT_FMTS % (result, LARGE_CASE, float(result)/(LARGE_CASE) * 100, elapse))

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

os.system('make clean > /dev/null')
os.system('make > /dev/null')

try:
    os.remove("dbi")
except:
    pass

print("-------------- Sequantial Insert Test --------------")
#test_case_seq("Small(2^10)", SMALL_CASE)
#test_case_seq("Medium(2^15)", MEDIUM_CASE)
#test_case_seq("Large(2^20)", LARGE_CASE)


print("--------------   Random Insert Test   --------------")
#test_case_rnd("Small(2^10)", SMALL_CASE)
test_case_rnd("Medium(2^15)", MEDIUM_CASE)
#test_case_rnd("Large(2^20)", LARGE_CASE)

print("--------------      Delete Test       --------------")
#test_delete_seq()
#test_delete_rev()
#test_delete_chunk()
#test_delete_random("Random_Delete_Small(2^10)", SMALL_CASE)
test_delete_random("Random_Delete_Medium(2^15)", MEDIUM_CASE)
#test_delete_random("Random_Delete_Large(2^19)", int(LARGE_CASE / 2))
#test_delete_random("Random_Delete_ALL(2^20)", LARGE_CASE)
