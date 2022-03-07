from utils_req import notify_line
from batch_Manager import BatchPast
from batch_Manager import BatchDay


def do_BatchPast(opdt, mid):
    BP = BatchPast(opdt)
    BP.main()


def do_BatchDay(opdt, mid):
    BP = BatchDay(opdt, mid)
    BP.main()



if __name__ == "__main__":
    opdt = '20220306'
    mid ="219"

    # do_BatchPast(opdt, mid)
    do_BatchDay(opdt, mid)

    notify_line('owatta nya-')